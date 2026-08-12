"use client";

import { Plus, X } from "lucide-react";
import { useTranslations } from "next-intl";
import { useRef, useState } from "react";
import { BaseUrlMode } from "@/api/endpoints/channel";
import {
  cloneSiteEndpointSet,
  deriveFollowSiteModelURL,
  resolveSiteDefaultEndpointSet,
  resolveSiteEndpointSet,
  type SiteEndpointSet,
  type SiteModelEndpointConfig,
  type SiteModelRouteType,
} from "@/api/endpoints/site";
import { Button } from "@/components/ui/button";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Badge } from "@/components/ui/badge";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { EndpointModeSelect, EndpointURLListEditor } from "@/components/modules/channel/EndpointSetEditor";

export const SITE_MODEL_ROUTE_OPTIONS: ReadonlyArray<{ value: SiteModelRouteType; label: string }> = [
  { value: "openai_chat", label: "OpenAI Chat" },
  { value: "openai_response", label: "OpenAI Responses" },
  { value: "anthropic", label: "Anthropic Messages" },
  { value: "gemini", label: "Gemini" },
  { value: "volcengine", label: "Volcengine" },
  { value: "openai_embedding", label: "OpenAI Embeddings" },
];

const ENDPOINT_MODE_KEYS: Record<
  BaseUrlMode,
  | "baseUrlModeDelay"
  | "baseUrlModeFailover"
  | "baseUrlModeRandom"
  | "baseUrlModeWeighted"
> = {
  [BaseUrlMode.Delay]: "baseUrlModeDelay",
  [BaseUrlMode.Failover]: "baseUrlModeFailover",
  [BaseUrlMode.Random]: "baseUrlModeRandom",
  [BaseUrlMode.Weighted]: "baseUrlModeWeighted",
};

type Props = {
  config: SiteModelEndpointConfig;
  baseURL: string;
  onChange: (config: SiteModelEndpointConfig) => void;
};

type DefaultSourceChange = {
  config: SiteModelEndpointConfig;
  customDraft?: SiteEndpointSet;
};

export function changeSiteDefaultEndpointSource(
  config: SiteModelEndpointConfig,
  source: "follow_site" | "custom",
  baseURL: string,
  customDraft?: SiteEndpointSet,
): DefaultSourceChange {
  const preservedCustomDraft = config.default.source === "custom"
    ? cloneSiteEndpointSet(config.default.endpoint_set)
    : customDraft
      ? cloneSiteEndpointSet(customDraft)
      : undefined;

  if (source === "follow_site") {
    return {
      config: { ...config, default: { source: "follow_site" } },
      customDraft: preservedCustomDraft,
    };
  }

  const endpointSet = cloneSiteEndpointSet(
    preservedCustomDraft ?? resolveSiteDefaultEndpointSet(config, baseURL).endpoint_set,
  );
  return {
    config: { ...config, default: { source: "custom", endpoint_set: endpointSet } },
    customDraft: cloneSiteEndpointSet(endpointSet),
  };
}

export function SiteEndpointConfigEditor({ config, baseURL, onChange }: Props) {
  const t = useTranslations("siteEndpoint");
  const channelT = useTranslations("channel.form");
  const [overridePickerOpen, setOverridePickerOpen] = useState(false);
  const defaultCustomDraftRef = useRef<SiteEndpointSet | undefined>(
    config.default.source === "custom"
      ? cloneSiteEndpointSet(config.default.endpoint_set)
      : undefined,
  );
  const usedRouteTypes = new Set(config.route_overrides.map((item) => item.route_type));
  const availableRouteOptions = SITE_MODEL_ROUTE_OPTIONS.filter(
    (option) => !usedRouteTypes.has(option.value),
  );
  const defaultResolved = resolveSiteDefaultEndpointSet(config, baseURL);
  const defaultCustomSet = config.default.source === "custom"
    ? config.default.endpoint_set
    : defaultResolved.endpoint_set;
  const updateDefaultSet = (endpoint_set: SiteEndpointSet) => {
    defaultCustomDraftRef.current = cloneSiteEndpointSet(endpoint_set);
    onChange({
      ...config,
      default: { source: "custom", endpoint_set: cloneSiteEndpointSet(endpoint_set) },
    });
  };
  const switchDefaultSource = (source: "follow_site" | "custom") => {
    const next = changeSiteDefaultEndpointSource(
      config,
      source,
      baseURL,
      defaultCustomDraftRef.current,
    );
    defaultCustomDraftRef.current = next.customDraft;
    onChange(next.config);
  };
  const addOverride = (route: SiteModelRouteType) => {
    if (usedRouteTypes.has(route)) return;
    const inherited = resolveSiteEndpointSet(config, route, baseURL).endpoint_set;
    onChange({
      ...config,
      route_overrides: [...config.route_overrides, { route_type: route, endpoint_set: cloneSiteEndpointSet(inherited) }],
    });
    setOverridePickerOpen(false);
  };

  return (
    <div className="space-y-5">
      <div className="space-y-3 rounded-xl border p-3">
        <div className="grid gap-3 md:grid-cols-2">
          <div className="space-y-2">
            <label htmlFor="site-default-endpoint-source" className="text-sm font-medium">
              {t("defaultSource")}
            </label>
            <Select value={config.default.source} onValueChange={(value) => switchDefaultSource(value as "follow_site" | "custom")}>
              <SelectTrigger id="site-default-endpoint-source" className="rounded-xl">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="follow_site">{t("followSite")}</SelectItem>
                <SelectItem value="custom">{t("custom")}</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="rounded-lg bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
            <div>
              {t("effectiveSource")}: {t(config.default.source === "follow_site" ? "followSite" : "custom")}
              <span aria-hidden="true"> &middot; </span>
              {channelT(ENDPOINT_MODE_KEYS[defaultResolved.endpoint_set.base_url_mode])}
            </div>
            {config.default.source === "follow_site" ? (
              <div className="mt-1 break-all">{deriveFollowSiteModelURL(baseURL)}</div>
            ) : null}
          </div>
        </div>
        {config.default.source === "custom" ? (
          <div className="space-y-4">
            <EndpointURLListEditor
              idPrefix="site-default-endpoint"
              endpoints={defaultCustomSet.base_urls}
              mode={defaultCustomSet.base_url_mode}
              createEndpoint={() => ({ url: "" })}
              onChange={(base_urls) => updateDefaultSet({ ...defaultCustomSet, base_urls })}
              label={t("apiBaseUrls")}
              urlLabel={t("apiBaseUrl")}
              placeholder={t("apiBaseUrlPlaceholder")}
              description={t("apiBaseUrlDescription")}
            />
            <EndpointModeSelect
              idPrefix="site-default-endpoint"
              value={defaultCustomSet.base_url_mode}
              onChange={(base_url_mode) => updateDefaultSet({ ...defaultCustomSet, base_url_mode })}
            />
          </div>
        ) : null}
      </div>

      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-sm font-medium">{t("protocolOverrides")}</div>
            <p className="text-xs text-muted-foreground/70">{t("overrideDescription")}</p>
          </div>
          <Popover open={overridePickerOpen} onOpenChange={setOverridePickerOpen}>
            <PopoverTrigger asChild>
              <Button type="button" variant="ghost" size="sm"
                disabled={availableRouteOptions.length === 0}
                className="h-7 px-2 text-xs text-muted-foreground hover:bg-transparent">
                <Plus className="mr-1 h-3 w-3" />{t("addOverride")}
              </Button>
            </PopoverTrigger>
            <PopoverContent align="end" className="w-56 rounded-xl p-1.5">
              <div className="px-2 py-1.5 text-xs font-medium text-muted-foreground">
                {t("protocol")}
              </div>
              <div className="grid gap-1">
                {availableRouteOptions.map((option) => (
                  <Button key={option.value} type="button" variant="ghost" size="sm"
                    onClick={() => addOverride(option.value)}
                    className="justify-start font-normal">
                    {option.label}
                  </Button>
                ))}
              </div>
            </PopoverContent>
          </Popover>
        </div>
        <Accordion type="single" collapsible className="rounded-xl border bg-muted/10">
          <AccordionItem value="effective-preview" className="border-none">
            <AccordionTrigger className="rounded-xl px-3 py-2.5 text-xs hover:bg-muted/30 hover:no-underline">
              {t("effectivePreview")}
            </AccordionTrigger>
            <AccordionContent className="border-t px-3 pb-3 pt-3">
              <div className="grid gap-2 md:grid-cols-2">
                {SITE_MODEL_ROUTE_OPTIONS.map((option) => {
                  const effective = resolveSiteEndpointSet(
                    config,
                    option.value,
                    baseURL,
                  );
                  const isOverride = effective.source === "route_override";
                  return (
                    <div
                      key={option.value}
                      className="rounded-lg border bg-background/70 px-3 py-2 text-xs"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="font-medium text-foreground">{option.label}</div>
                        <Badge variant={isOverride ? "default" : "secondary"}>
                          {t(isOverride ? "routeOverride" : "inheritDefault")}
                        </Badge>
                      </div>
                      <div className="mt-1 text-muted-foreground">
                        {channelT(ENDPOINT_MODE_KEYS[effective.endpoint_set.base_url_mode])}
                      </div>

                    </div>
                  );
                })}
              </div>
            </AccordionContent>
          </AccordionItem>
        </Accordion>
        {config.route_overrides.map((override, index) => {
          const routeLabel = SITE_MODEL_ROUTE_OPTIONS.find(
            (option) => option.value === override.route_type,
          )?.label ?? override.route_type;
          return (
            <div key={override.route_type} className="space-y-3 rounded-xl border p-3">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <div className="truncate text-sm font-medium text-foreground">
                    {routeLabel}
                  </div>
                  <div className="mt-1 flex flex-wrap items-center gap-2">
                    <Badge variant="outline">{t("routeOverride")}</Badge>
                    <span className="text-xs text-muted-foreground">{t("completeReplacement")}</span>
                  </div>
                </div>
                <Button type="button" variant="ghost" size="sm"
                  onClick={() => onChange({ ...config, route_overrides: config.route_overrides.filter((_, current) => current !== index) })}
                  className="h-8 w-8 p-0 text-muted-foreground hover:bg-transparent hover:text-destructive"
                  title={t("removeOverride")}
                  aria-label={t("removeOverride")}>
                  <X className="h-4 w-4" />
                </Button>
              </div>
              <div className="space-y-4">
                <EndpointURLListEditor
                  idPrefix={`site-${override.route_type}-${index}`}
                  endpoints={override.endpoint_set.base_urls}
                  mode={override.endpoint_set.base_url_mode}
                  createEndpoint={() => ({ url: "" })}
                  onChange={(base_urls) => onChange({ ...config, route_overrides: config.route_overrides.map((item, current) =>
                    current === index ? { ...item, endpoint_set: { ...item.endpoint_set, base_urls } } : item) })}
                  label={t("apiBaseUrls")}
                  urlLabel={t("apiBaseUrl")}
                  placeholder={t("apiBaseUrlPlaceholder")}
                  description={t("apiBaseUrlDescription")}
                />
                <EndpointModeSelect
                  idPrefix={`site-${override.route_type}-${index}`}
                  value={override.endpoint_set.base_url_mode}
                  onChange={(base_url_mode) => onChange({ ...config, route_overrides: config.route_overrides.map((item, current) =>
                    current === index ? { ...item, endpoint_set: { ...item.endpoint_set, base_url_mode } } : item) })}
                />
              </div>
            </div>
          );
        })}
        {config.route_overrides.length === 0 ? (
          <div className="rounded-xl border border-dashed px-3 py-4 text-center text-xs text-muted-foreground">
            {t("allInheritDefault")}
          </div>
        ) : null}
      </div>
    </div>
  );
}
