import type { LucideIcon } from "lucide-react";
import {
  Activity,
  BarChart3,
  CalendarClock,
  Database,
  FlaskConical,
  Store,
  Workflow,
} from "lucide-react";

import { CONNECTIONS_HREF } from "@/app/connections/routes";
import { MARKETPLACE_HREF } from "@/app/marketplace/routes";
import { SOURCES_HREF } from "@/app/sources/routes";

/** One entry of the left sidebar. */
export interface NavItem {
  href: string;
  label: string;
  icon: LucideIcon;
  description: string;
}

/**
 * The screens of the admin UI.
 */
export const NAV_ITEMS: NavItem[] = [
  {
    href: MARKETPLACE_HREF,
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
  {
    href: SOURCES_HREF,
    label: "Sources",
    icon: CalendarClock,
    description: "Fetch a URL on a schedule and feed it to a pipeline",
  },
  {
    href: CONNECTIONS_HREF,
    label: "Connections",
    icon: Database,
    description: "Named Meilisearch destinations pipelines can pin",
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
