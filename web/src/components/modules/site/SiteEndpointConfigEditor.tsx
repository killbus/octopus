"use client";

import { Plus, X } from "lucide-react";
import { useTranslations } from "next-intl";
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

export function SiteEndpointConfigEditor({ config, baseURL, onChange }: Props) {
  const t = useTranslations("siteEndpoint");
  const channelT = useTranslations("channel.form");
  const defaultResolved = resolveSiteDefaultEndpointSet(config, baseURL);
  const defaultCustomSet = config.default.source === "custom"
    ? config.default.endpoint_set
    : defaultResolved.endpoint_set;
  const updateDefaultSet = (endpoint_set: SiteEndpointSet) =>
    onChange({ ...config, default: { source: "custom", endpoint_set } });
  const switchDefaultSource = (source: "follow_site" | "custom") => {
    onChange({
      ...config,
      default: source === "follow_site"
        ? { source: "follow_site" }
        : { source: "custom", endpoint_set: cloneSiteEndpointSet(defaultResolved.endpoint_set) },
    });
  };
  const addOverride = () => {
    const used = new Set(config.route_overrides.map((item) => item.route_type));
    const route = SITE_MODEL_ROUTE_OPTIONS.find((item) => !used.has(item.value))?.value;
    if (!route) return;
    const inherited = resolveSiteEndpointSet(config, route, baseURL).endpoint_set;
    onChange({
      ...config,
      route_overrides: [...config.route_overrides, { route_type: route, endpoint_set: cloneSiteEndpointSet(inherited) }],
    });
  };

  return (
    <div className="space-y-5">
      <div className="space-y-3 rounded-xl border p-3">
        <div className="grid gap-3 md:grid-cols-2">
          <div className="space-y-2">
            <label className="text-sm font-medium">{t("defaultSource")}</label>
            <Select value={config.default.source} onValueChange={(value) => switchDefaultSource(value as "follow_site" | "custom")}>
              <SelectTrigger className="rounded-xl"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="follow_site">{t("followSite")}</SelectItem>
                <SelectItem value="custom">{t("custom")}</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="rounded-lg bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
            <div>{t("effectiveSource")}: {t(config.default.source === "follow_site" ? "followSite" : "custom")}</div>
            <div className="mt-1 break-all">
              {config.default.source === "follow_site"
                ? deriveFollowSiteModelURL(baseURL)
                : config.default.endpoint_set.base_urls.map((item) => item.url).join(", ")}
            </div>
          </div>
        </div>
        {config.default.source === "custom" ? (
          <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_minmax(12rem,0.45fr)]">
            <EndpointURLListEditor
              idPrefix="site-default-endpoint"
              endpoints={defaultCustomSet.base_urls}
              mode={defaultCustomSet.base_url_mode}
              createEndpoint={() => ({ url: "" })}
              onChange={(base_urls) => updateDefaultSet({ ...defaultCustomSet, base_urls })}
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
          <Button type="button" variant="ghost" size="sm" onClick={addOverride}
            disabled={config.route_overrides.length >= SITE_MODEL_ROUTE_OPTIONS.length}
            className="h-7 px-2 text-xs text-muted-foreground hover:bg-transparent">
            <Plus className="mr-1 h-3 w-3" />{t("addOverride")}
          </Button>
        </div>
        <div className="grid gap-2 md:grid-cols-2">
          {SITE_MODEL_ROUTE_OPTIONS.map((option) => {
            const effective = resolveSiteEndpointSet(
              config,
              option.value,
              baseURL,
            );
            const sourceLabel =
              effective.source === "route_override"
                ? t("completeReplacement")
                : effective.source === "default_custom"
                  ? t("custom")
                  : t("followSite");
            return (
              <div
                key={option.value}
                className="rounded-lg border bg-muted/20 px-3 py-2 text-xs"
              >
                <div className="font-medium text-foreground">{option.label}</div>
                <div className="mt-1 text-muted-foreground">
                  {t("effectiveSource")}: {sourceLabel} � {channelT(ENDPOINT_MODE_KEYS[effective.endpoint_set.base_url_mode])}
                </div>
                <div className="mt-1 break-all text-muted-foreground">
                  {effective.endpoint_set.base_urls
                    .map((endpoint) => endpoint.url)
                    .join(", ")}
                </div>
              </div>
            );
          })}
        </div>
        {config.route_overrides.map((override, index) => (
          <div key={`${override.route_type}-${index}`} className="space-y-3 rounded-xl border p-3">
            <div className="flex items-center gap-2">
              <Select value={override.route_type} onValueChange={(value) =>
                onChange({ ...config, route_overrides: config.route_overrides.map((item, current) =>
                  current === index ? { ...item, route_type: value as SiteModelRouteType } : item) })}>
                <SelectTrigger className="w-48 rounded-xl"><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SITE_MODEL_ROUTE_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value}
                      disabled={config.route_overrides.some((item, current) => current !== index && item.route_type === option.value)}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <div className="flex-1 text-xs text-muted-foreground">{t("completeReplacement")}</div>
              <Button type="button" variant="ghost" size="sm"
                onClick={() => onChange({ ...config, route_overrides: config.route_overrides.filter((_, current) => current !== index) })}
                className="h-8 w-8 p-0 text-muted-foreground hover:bg-transparent hover:text-destructive"
                title={t("removeOverride")}>
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="rounded-lg bg-muted/40 px-3 py-2 text-xs text-muted-foreground">
              <div>
                {t("effectiveSource")}: {t("completeReplacement")}
              </div>
              <div className="mt-1 break-all">
                {override.endpoint_set.base_urls
                  .map((endpoint) => endpoint.url)
                  .join(", ")}
              </div>
            </div>
            <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_minmax(12rem,0.45fr)]">
              <EndpointURLListEditor
                idPrefix={`site-${override.route_type}-${index}`}
                endpoints={override.endpoint_set.base_urls}
                mode={override.endpoint_set.base_url_mode}
                createEndpoint={() => ({ url: "" })}
                onChange={(base_urls) => onChange({ ...config, route_overrides: config.route_overrides.map((item, current) =>
                  current === index ? { ...item, endpoint_set: { ...item.endpoint_set, base_urls } } : item) })}
              />
              <EndpointModeSelect
                idPrefix={`site-${override.route_type}-${index}`}
                value={override.endpoint_set.base_url_mode}
                onChange={(base_url_mode) => onChange({ ...config, route_overrides: config.route_overrides.map((item, current) =>
                  current === index ? { ...item, endpoint_set: { ...item.endpoint_set, base_url_mode } } : item) })}
              />
            </div>
          </div>
        ))}
        {config.route_overrides.length === 0 ? (
          <div className="rounded-xl border border-dashed px-3 py-4 text-center text-xs text-muted-foreground">
            {t("allInheritDefault")}
          </div>
        ) : null}
      </div>
    </div>
  );
}
