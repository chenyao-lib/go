# Client RPC payload encryption

Authentication messages and authentication responses are plaintext. After authentication succeeds, every RPC message data field is encoded by the connection's PayloadCodec.

The outer RPC fields remain JSON so the transport can route requests and responses:

```json
{
  "session": "machine-1-1",
  "method": "scan_qrcode_pay",
  "data": {
    "v": 1,
    "alg": "aes-gcm-v1",
    "kid": "default",
    "nonce": "<base64>",
    "ciphertext": "<base64>"
  },
  "error": ""
}
```

For aes-gcm-v1:

- nonce is a fresh random standard GCM nonce for each message.
- ciphertext contains the GCM ciphertext and authentication tag.
- The AES key is Base64-encoded and must decode to 16, 24, or 32 bytes.
- Plaintext and encrypted payloads are limited to 1 MiB and 2 MiB respectively.
- Version, algorithm, key ID, client ID, session, method, error, and direction are authenticated as additional data.
- Direction is c2s for client-to-server traffic and s2c for server-to-client traffic.
- A missing business result is encrypted as JSON null, so error responses are authenticated too.

The client must build exactly the same authenticated metadata. Authentication failure, metadata changes, an unexpected key ID, or ciphertext changes must reject the message.

To add another algorithm, implement payloadcodec.Codec in this package and extend NewFactory. Neither srv.Client, clt.WSClient, binding, nor business services need to change.

Client setup:

```go
client := clt.NewClient(ctx, host, clientType, clientID)

codec, err := payloadcodec.NewAESGCMCodec(base64Key, keyID)
if err != nil {
	return err
}
if err := client.SetPayloadCodec(codec); err != nil {
	return err
}

client.Start(onConnected)
```

Set the codec before `Start`. The client and server must use the same algorithm, key, and key ID.

In production these values are no longer global configuration. `redeemcenter`
generates a fresh AES-256 key and key ID for every `/allocate` response, stores
them with the token in `machine_token`, and returns them in `client_crypto`.
After token validation, `redeemserver` creates the connection codec from that
database row.
