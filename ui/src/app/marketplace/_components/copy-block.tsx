"use client";

import { Check, Copy } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";

/** A read-only code block with a copy button. */
export function CopyBlock({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard access is denied in some browsers and over plain HTTP; say so
      // rather than leaving the button looking broken.
      toast.error("Could not copy", { description: "Select the text and copy it manually." });
    }
  }

  return (
    <div className="relative">
      <pre className="overflow-x-auto rounded-md border bg-muted/40 p-3 text-xs">
        <code>{text}</code>
      </pre>
      <Button
        type="button"
        size="sm"
        variant="ghost"
        onClick={copy}
        aria-label={label}
        className="absolute top-1.5 right-1.5"
      >
        {copied ? <Check className="size-3.5" aria-hidden /> : <Copy className="size-3.5" aria-hidden />}
      </Button>
    </div>
  );
}
