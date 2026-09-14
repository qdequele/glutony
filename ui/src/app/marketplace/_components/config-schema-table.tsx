import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { JsonSchema } from "@/lib/api/types";

/** `["integer", "null"]` → `integer | null`. */
function typeLabel(schema: JsonSchema): string {
  const type = schema.type;
  if (Array.isArray(type)) return type.join(" | ");
  return type ?? "any";
}

function defaultLabel(schema: JsonSchema): string {
  if (!("default" in schema) || schema.default === undefined) return "—";
  return JSON.stringify(schema.default);
}

/**
 * A plugin's `config:` block, read-only.
 *
 * Properties marked `readOnly` are injected by the workflow at run time — that
 * is how `meili_indexer` declares `host`, `api_key` and `index`, one of which is
 * a secret. They are shown, because knowing they exist matters, but labelled so
 * nobody tries to set them by hand.
 */
export function ConfigSchemaTable({ schema }: { schema: JsonSchema | undefined }) {
  const properties = Object.entries(schema?.properties ?? {});
  const required = new Set(schema?.required ?? []);

  if (properties.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">
        This action takes no configuration.
      </p>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Key</TableHead>
            <TableHead>Type</TableHead>
            <TableHead>Default</TableHead>
            <TableHead>Description</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {properties.map(([key, property]) => (
            <TableRow key={key}>
              <TableCell className="align-top font-mono text-xs">
                {key}
                {required.has(key) ? (
                  <Badge variant="outline" className="ml-1.5">
                    required
                  </Badge>
                ) : null}
                {property.readOnly ? (
                  <Badge variant="secondary" className="ml-1.5">
                    set by the system
                  </Badge>
                ) : null}
              </TableCell>
              <TableCell className="align-top font-mono text-xs">
                {typeLabel(property)}
              </TableCell>
              <TableCell className="align-top font-mono text-xs">
                {defaultLabel(property)}
              </TableCell>
              <TableCell className="align-top text-xs text-muted-foreground">
                {property.description ?? "—"}
                {property.enum ? (
                  <span className="mt-1 block font-mono">
                    one of {property.enum.map((value) => JSON.stringify(value)).join(", ")}
                  </span>
                ) : null}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
