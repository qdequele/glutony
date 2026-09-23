import { KeyRound } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";

/**
 * What the sources and connections screens show when the gateway answers
 * `501 not_configured`: both features seal secrets with `SOURCE_SECRET_KEY`,
 * and refuse to run without one. Not an error — a deployment choice.
 */
export function NotEnabled({ feature }: { feature: string }) {
  return (
    <Alert>
      <KeyRound aria-hidden />
      <AlertTitle>{feature} are not enabled on this deployment</AlertTitle>
      <AlertDescription>
        <p>
          The gateway stores their credentials encrypted and needs a key to do so. Set{" "}
          <span className="font-mono">SOURCE_SECRET_KEY</span> on the gateway and restart it to
          turn scheduled sources and Meilisearch connections on.
        </p>
      </AlertDescription>
    </Alert>
  );
}
