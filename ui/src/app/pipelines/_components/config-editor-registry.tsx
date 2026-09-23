import type { ConfigEditorEntry } from "./config-editors";
import { DocumentScriptEditor } from "./document-script/document-script-editor";

/** Plugins whose config gets a dedicated editor instead of (part of) the generated form. */
export const CONFIG_EDITORS: Record<string, ConfigEditorEntry> = {
  document_script: { Editor: DocumentScriptEditor, ownedKeys: ["script", "ops"] },
};
