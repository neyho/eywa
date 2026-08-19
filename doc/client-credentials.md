# client_credentials

How a machine authenticates to EYWA. OAuth 2.0 §4.4 — no user, no browser, no
redirect: the client presents its own credentials and gets an access token.

Implemented in `src/clj/neyho/eywa/iam/oauth/token.clj` (the grant) and
`src/clj/neyho/eywa/iam.clj` (the identity and the secret).

## Identity is a SERVICE user

**The identity of a client_credentials client is the `User` whose `name` equals
the OAuth Client's `id`.**

That is the whole design. Roles hang off that user, so a service's permissions
are administered in IAM exactly like a person's — assign roles to it and the
service gets precisely those. Nothing downstream (RBAC, RLS, `acting_as`,
audit) has to learn that the caller isn't human.

The user is provisioned by `iam/ensure-service-user`, a post-`@hook` in
`resources/iam.graphql` on **all four** client write mutations — `syncOAuthClient`,
`stackOAuthClient`, and both `*List` forms. The List variants are load-bearing:
the Apps card commits through `stackOAuthClientList`, so hooking only the
singular forms means a UI-created client has no identity and fails at the token
endpoint with correct credentials. An existing user is left completely alone, so
the hook never clobbers role assignments.

Roles are resolved **at grant time**. Changing a service's roles needs a fresh
token.

## Session-backed, not stateless

`grant-token "client_credentials"` mints a persistent `:flow
"client_credentials"` session and calls the ordinary `token/generate`. Issued
tokens are normal store-registered access tokens, so the auth interceptor,
websockets, introspection, revocation and the session sweeper all work
untouched. Repeat grants reuse one session per client.

The service user is read fresh on every grant rather than through
`core/get-resource-owner` — that cache has no invalidation, so a deactivated
service would keep getting tokens. One extra read per token lifetime.

## No refresh token

RFC 6749 §4.4.3. The client already holds credentials that mint tokens, so a
refresh token buys nothing and is one more secret to leak. `offline_access` is
stripped from the requested scope rather than refused, so a client that asks
still gets a working token.

## Validation

`validate-client-credentials` refuses, in order — all as `401 invalid_client`,
so a caller can't probe which condition it tripped:

| condition | why |
|---|---|
| `active` is `false` | deactivating is the kill switch an admin reaches for. `false?`, not `not` — clients predating the flag have `nil` and must keep working |
| type is `public` | no secret to authenticate with; §4.4 is confidential-only |
| `client_credentials` not in `allowed-grants` | opt-in per client |
| secret doesn't match | bcrypt+sha512 compare |

Then, separately: no active service user → `401 invalid_client`.

## The secret

`OAuthClient.Secret` is a `hashed` attribute (IAM 0.80.2 / OAuth Session 0.1.4
— the entity is defined in **both** datasets and they must agree, or whichever
deploys last wins). `OAuthClient.secret` always resolves to `null` via
`hide-client-secret`.

Rotation is its own mutation, `regenerateOAuthClientSecret`, `@protect`ed by
`iam.client:rotate-secret`. It writes immediately, returns the plaintext once,
and **kills the client's live sessions** — a rotation is normally a response to
a suspected leak, so the old secret losing the ability to buy new tokens isn't
enough; tokens it already bought would stay valid for up to the access-token
lifetime.

It is deliberately not a field on the client record. Riding in card state would
park a live secret in the caller's form until they remember to save — and a
caller who copied it and then discarded would walk away with a secret the server
never stored, while the old one silently kept working.

## Wire

Credentials go in the **form body only**; there is no `client_secret_basic`
(HTTP Basic) support.

```
POST /oauth/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id=…&client_secret=…
```

## Verifying it

`examples/js/client-credentials/` — zero-dependency TypeScript, runs on
Node 23+ with no build step.

- `demo-backend-conformance.ts` — 21 checks over the grant, the six rejection
  paths, SERVICE-user provisioning and deactivation, and rotation killing live
  tokens. Creates and deletes its own fixtures; exits non-zero on failure.
- `demo-stay-connected.ts` / `demo-expiry-watch.ts` — a service holding a
  6–10s token across many renewals, asserting headroom never goes negative.

Bootstrapping the admin client those demos need has no UI path (you need
credentials to create credentials). Over nREPL:

```clojure
(dataset/sync-entity iu/app {:id id :type "confidential" :active true
                             :secret (gen/client-secret)
                             :settings {"allowed-grants" ["client_credentials"]}})
(iam/ensure-service-user! id [{:name "SUPERUSER"}])
```

OAuth clients live in the `app` entity (`neyho.eywa.iam.uuids/app`).
