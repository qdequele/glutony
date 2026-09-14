import type { LucideIcon } from "lucide-react";
import { Activity, BarChart3, FlaskConical, Store, Workflow } from "lucide-react";

/** One entry of the left sidebar. */
export interface NavItem {
  href: string;
  label: string;
  icon: LucideIcon;
  description: string;
}

/**
 * The five screens of the admin UI.
 */
export const NAV_ITEMS: NavItem[] = [
  {
    href: "/marketplace",
    label: "Marketplace",
    icon: Store,
    description: "Browse every action and workflow the system ships with",
  },
  {
    href: "/pipelines",
    label: "Pipelines",
    icon: Workflow,
    description: "Author and validate ingestion pipelines",
  },
  { href: "/jobs", label: "Jobs", icon: Activity, description: "Track ingestion jobs" },
  {
    href: "/playground",
    label: "Playground",
    icon: FlaskConical,
    description: "Send content through a pipeline",
  },
  { href: "/usage", label: "Usage", icon: BarChart3, description: "Per-tenant metering" },
];
