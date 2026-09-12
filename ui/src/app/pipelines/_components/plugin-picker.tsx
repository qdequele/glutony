"use client";

import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { PluginKind, PluginManifest } from "@/lib/api/types";

const GROUP_ORDER: PluginKind[] = ["builtin", "wasm", "grpc"];
const GROUP_LABEL: Record<PluginKind, string> = {
  builtin: "Built-in",
  wasm: "WASM",
  grpc: "gRPC",
};

/** Plugin `<Select>`, grouped by execution kind, with each manifest's description. */
export function PluginPicker({
  id,
  value,
  plugins,
  onChange,
  disabled,
}: {
  id: string;
  value: string;
  plugins: PluginManifest[];
  onChange: (name: string) => void;
  disabled?: boolean;
}) {
  const groups = GROUP_ORDER.map((kind) => ({
    kind,
    plugins: plugins.filter((plugin) => (plugin.kind ?? "builtin") === kind),
  })).filter((group) => group.plugins.length > 0);

  // A pipeline can reference a plugin no worker registered; keep it selectable
  // so opening such a pipeline does not silently rewrite it.
  const missing = value.length > 0 && !plugins.some((plugin) => plugin.name === value);

  return (
    <Select value={value || undefined} onValueChange={onChange} disabled={disabled}>
      <SelectTrigger id={id} className="w-full font-mono">
        <SelectValue placeholder="Select a plugin…">
          {value ? value : undefined}
        </SelectValue>
      </SelectTrigger>
      <SelectContent className="max-h-80">
        {missing ? (
          <SelectGroup>
            <SelectLabel>Not registered</SelectLabel>
            <SelectItem value={value}>
              <div className="flex flex-col gap-0.5">
                <span className="font-mono">{value}</span>
                <span className="text-xs text-muted-foreground">
                  no worker published this manifest
                </span>
              </div>
            </SelectItem>
          </SelectGroup>
        ) : null}
        {groups.map((group) => (
          <SelectGroup key={group.kind}>
            <SelectLabel>{GROUP_LABEL[group.kind]}</SelectLabel>
            {group.plugins.map((plugin) => (
              <SelectItem key={plugin.name} value={plugin.name}>
                <div className="flex flex-col gap-0.5 py-0.5">
                  <span className="font-mono">{plugin.name}</span>
                  <span className="max-w-80 text-xs text-muted-foreground">
                    {plugin.description || "no description"}
                  </span>
                  <span className="text-[11px] text-muted-foreground/80">
                    accepts {(plugin.accepts ?? []).join(", ") || "nothing"} → produces{" "}
                    {plugin.produces}
                  </span>
                </div>
              </SelectItem>
            ))}
          </SelectGroup>
        ))}
      </SelectContent>
    </Select>
  );
}
