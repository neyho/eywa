(ns neyho.eywa.server.ws.graphql
  (:require
    [clojure.string :as str]
    [neyho.eywa.server.interceptors.authentication
     :refer [get-token-context]]
    [io.pedestal.interceptor :refer [interceptor]]
    [neyho.eywa.lacinia :as lacinia]
    [neyho.eywa.iam.access.context :refer [*user* *groups* *roles* *rls*]]
    [io.pedestal.interceptor.chain :as chain]
    [com.walmartlabs.lacinia.validator :as validator]
    [com.walmartlabs.lacinia.internal-utils :refer [to-message]]
    [com.walmartlabs.lacinia.parser :as parser]
    [io.pedestal.http :as http]
    [com.walmartlabs.lacinia.pedestal.subscriptions :as subscriptions])
  (:import
    [jakarta.websocket EndpointConfig Session]))


(defn ^:private on-leave-query-parser
  [context]
  (update context :request dissoc :parsed-lacinia-query))

(defn ^:private add-error
  [context exception]
  (assoc context ::chain/error exception))

(defn ^:private on-error-query-parser
  [context exception]
  (-> (on-leave-query-parser context)
      (add-error exception)))


(def query-parser-interceptor
  (interceptor
    {:name ::query-parser
     :enter (fn [context]
              (let [{operation-name :operationName
                     :keys [query variables]} (:request context)
                    actual-schema (deref lacinia/compiled)
                    parsed-query (try
                                   (parser/parse-query actual-schema query operation-name)
                                   (catch Throwable t
                                     (throw (ex-info (to-message t)
                                                     {::errors (-> t ex-data :errors)}
                                                     t))))
                    prepared (parser/prepare-with-query-variables parsed-query variables)
                    errors (validator/validate actual-schema prepared {})]
                (if (seq errors)
                  (throw (ex-info "Query validation errors." {::errors errors}))
                  (assoc-in context [:request :parsed-lacinia-query] prepared))))
     :leave on-leave-query-parser
     :error on-error-query-parser}))


(defn ^:private bearer-from-header
  "Strip a leading 'Bearer ' from a header value. Returns nil for blank input."
  [v]
  (when (string? v)
    (let [trimmed (str/trim v)]
      (when-not (str/blank? trimmed)
        (str/replace trimmed #"(?i)^bearer\s+" "")))))

(defn ^:private extract-bearer
  "Pull a bearer token out of a graphql-ws connection_init payload. The spec
  says the payload is arbitrary, so we accept the shapes we see in the wild:

    {\"Authorization\"           \"Bearer …\"}
    {\"authorization\"           \"Bearer …\"}
    {\"headers\" {\"Authorization\" \"Bearer …\"}}      ;; Apollo-style
    {\"access_token\"            \"…\"}
    {\"authToken\"               \"…\"}                  ;; common shorthand"
  [payload]
  (when (map? payload)
    (or (bearer-from-header (or (get payload "Authorization")
                                (get payload "authorization")
                                (get payload :Authorization)
                                (get payload :authorization)))
        (extract-bearer (or (get payload "headers")
                            (get payload :headers)))
        (let [raw (or (get payload "access_token")
                      (get payload :access_token)
                      (get payload "authToken")
                      (get payload :authToken))]
          (when (and (string? raw) (not (str/blank? raw)))
            raw)))))


(defn ^:private url-token
  "Bearer token presented at the WS handshake via `?access_token=…`."
  [^Session session]
  (let [params (into {} (.getRequestParameterMap session))
        [token] (get params "access_token")]
    (when (and (string? token) (not (str/blank? token)))
      token)))


(defn ^:private context-from-token
  [token]
  (when token (get-token-context token)))


(defn listeners
  [{:keys [app-context]}]
  (let [wrapped-execute
        (interceptor
          {:name (:name subscriptions/execute-operation-interceptor)
           :leave (:leave subscriptions/execute-operation-interceptor)
           :error (:error subscriptions/execute-operation-interceptor)
           :enter
           (fn [{:as ctx
                 :keys [connection-params]
                 user :eywa/user
                 groups :eywa/groups
                 roles :eywa/roles
                 rls :eywa/rls}]
             ;; Prefer the user context attached at handshake (URL token).
             ;; Fall back to whatever the client sent in `connection_init`
             ;; payload so modern graphql-ws / graphql-transport-ws clients
             ;; (Apollo, urql, Relay) work without putting access tokens
             ;; in the URL.
             (let [ctx' (if user
                          ctx
                          (let [token (extract-bearer connection-params)
                                tctx  (context-from-token token)]
                            (cond-> ctx
                              tctx (merge tctx))))]
               (binding [*user*   (or (:eywa/user ctx')   user)
                         *groups* (or (:eywa/groups ctx') groups)
                         *roles*  (or (:eywa/roles ctx')  roles)
                         *rls*    (or (:eywa/rls ctx')    rls)]
                 ((:enter subscriptions/execute-operation-interceptor) ctx'))))})]
    [subscriptions/exception-handler-interceptor
     subscriptions/send-operation-response-interceptor
     query-parser-interceptor
     (subscriptions/inject-app-context-interceptor app-context)
     wrapped-execute]))


(defn endpoint
  [options]
  (assoc
    (subscriptions/subscription-websocket-endpoint
      nil
      {:subscription-interceptors (listeners options)
       ;; Don't close the socket here. Auth may legitimately arrive in
       ;; `connection_init` payload after the handshake (the spec-compliant
       ;; path for browser clients). The per-operation interceptor above
       ;; rejects any subscribe attempt that still has no user context.
       ;;
       ;; We DO pre-populate context from URL `?access_token=…` for
       ;; backwards compatibility with reachers and the CLI, which use
       ;; the URL fast path.
       :context-initializer
       (fn [ctx ^Session session]
         (let [user-ctx (context-from-token (url-token session))]
           (cond-> ctx user-ctx (merge user-ctx))))
       :keep-alive-ms 30000})
    :subprotocols ["graphql-ws" "graphql-transport-ws"]))


(defn enable
  ([service-map] (enable service-map nil))
  ([service-map options]
   (assoc-in service-map [::http/websockets "/graphql-ws"]
             (endpoint options))))
