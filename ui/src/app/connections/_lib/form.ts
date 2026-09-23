/**
 * Connection form values → API bodies. The API key is write-only: an empty key
 * input on edit means "keep the stored one", so it is simply not sent.
 */
import type {
  ConnectionView,
  CreateConnectionBody,
  UpdateConnectionBody,
} from "@/lib/api/connections";

/** Everything the connection dialog edits. */
export interface ConnectionFormValues {
  uid: string;
  name: string;
  host: string;
  apiKey: string;
}

/** A blank form. */
export function emptyConnectionFormValues(): ConnectionFormValues {
  return { uid: "", name: "", host: "", apiKey: "" };
}

/** The form for an existing connection. The key input starts empty. */
export function connectionToFormValues(connection: ConnectionView): ConnectionFormValues {
  return {
    uid: connection.uid,
    // The gateway defaults the name to the uid; show it only when it says more.
    name: connection.name === connection.uid ? "" : connection.name,
    host: connection.host,
    apiKey: "",
  };
}

/** `POST /connections` body. */
export function buildCreateConnectionBody(values: ConnectionFormValues): CreateConnectionBody {
  const body: CreateConnectionBody = {
    uid: values.uid.trim(),
    host: values.host.trim(),
    api_key: values.apiKey,
  };
  const name = values.name.trim();
  if (name) body.name = name;
  return body;
}

/** Hosts compare equal once whitespace and trailing slashes are ignored, as the gateway does. */
function normalizeHost(host: string): string {
  return host.trim().replace(/\/+$/, "");
}

/**
 * The smallest `PATCH /connections/{uid}` body. The gateway ignores a blank
 * name, so a cleared name is sent as the uid (its default). An empty object
 * means there is nothing to save.
 */
export function buildUpdateConnectionBody(
  stored: ConnectionView,
  values: ConnectionFormValues,
): UpdateConnectionBody {
  const body: UpdateConnectionBody = {};
  const name = values.name.trim() || stored.uid;
  if (name !== stored.name) body.name = name;
  if (normalizeHost(values.host) !== normalizeHost(stored.host)) body.host = values.host.trim();
  if (values.apiKey.length > 0) body.api_key = values.apiKey;
  return body;
}
