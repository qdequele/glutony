import type { SchemaWidgets } from "@/lib/schema-form";
import { ConnectionSelect } from "./connection-select";
import { JevQuestionsEditor } from "./jev-questions-editor";

/**
 * Widgets the step editor hands to every step's config form, keyed by the
 * `format` a plugin's `config_schema` gives a property.
 */
export const STEP_CONFIG_WIDGETS: SchemaWidgets = {
  "meili-connection": ConnectionSelect,
  "jev-questions": JevQuestionsEditor,
};
