import { renderToStaticMarkup } from "react-dom/server";
import { NextIntlClientProvider } from "next-intl";
import { describe, expect, it, vi } from "vitest";
import { BaseUrlMode } from "@/api/endpoints/channel";
import type { SiteModelEndpointConfig } from "@/api/endpoints/site";
import enMessages from "../../../../public/locale/en.json";
import {
  changeSiteDefaultEndpointSource,
  SiteEndpointConfigEditor,
} from "./SiteEndpointConfigEditor";

describe("SiteEndpointConfigEditor custom API base contract", () => {
  it("preserves the unsaved custom default across a follow-site round trip", () => {
    const original: SiteModelEndpointConfig = {
      default: {
        source: "custom",
        endpoint_set: {
          base_url_mode: BaseUrlMode.Weighted,
          base_urls: [
            { url: "https://primary.example/custom/v1", weight: 3 },
            { url: "https://backup.example/proxy", weight: 1 },
          ],
        },
      },
      route_overrides: [],
    };

    const followSite = changeSiteDefaultEndpointSource(
      original,
      "follow_site",
      "https://control.example",
    );
    expect(followSite.config.default).toEqual({ source: "follow_site" });

    const restored = changeSiteDefaultEndpointSource(
      followSite.config,
      "custom",
      "https://changed-control.example",
      followSite.customDraft,
    );
    expect(restored.config.default).toEqual(original.default);
    expect(restored.config.default).not.toBe(original.default);
    if (restored.config.default.source === "custom") {
      expect(restored.config.default.endpoint_set).not.toBe(original.default.endpoint_set);
    }
  });

  it("seeds the initial custom default from the effective base URL when no earlier custom draft exists", () => {
    const followSiteConfig: SiteModelEndpointConfig = {
      default: { source: "follow_site" },
      route_overrides: [],
    };

    const custom = changeSiteDefaultEndpointSource(
      followSiteConfig,
      "custom",
      "https://control.example/base",
      undefined,
      "https://control.example/base/v1",
    );

    expect(custom.config.default).toEqual({
      source: "custom",
      endpoint_set: {
        base_url_mode: BaseUrlMode.Delay,
        base_urls: [{ url: "https://control.example/base/v1" }],
      },
    });
  });

  it("falls back to the raw base URL when seeding custom and no effective URL is known", () => {
    const followSiteConfig: SiteModelEndpointConfig = {
      default: { source: "follow_site" },
      route_overrides: [],
    };

    const custom = changeSiteDefaultEndpointSource(
      followSiteConfig,
      "custom",
      "https://control.example/base",
    );

    expect(custom.config.default).toEqual({
      source: "custom",
      endpoint_set: {
        base_url_mode: BaseUrlMode.Delay,
        base_urls: [{ url: "https://control.example/base" }],
      },
    });
  });

  it("explains the exact API Base URL semantics without rewriting configured URLs", () => {
    const defaultURL =
      "https://gateway.example/proxy?signature=a/+%2F&token=x/";
    const overrideURL = "https://versionless.example";
    const config: SiteModelEndpointConfig = {
      default: {
        source: "custom",
        endpoint_set: {
          base_url_mode: BaseUrlMode.Delay,
          base_urls: [{ url: defaultURL }],
        },
      },
      route_overrides: [
        {
          route_type: "anthropic",
          endpoint_set: {
            base_url_mode: BaseUrlMode.Delay,
            base_urls: [{ url: overrideURL }],
          },
        },
      ],
    };
    const onChange = vi.fn();

    const html = renderToStaticMarkup(
      <NextIntlClientProvider locale="en" messages={enMessages} timeZone="UTC">
        <SiteEndpointConfigEditor
          config={config}
          baseURL="https://control.example"
          onChange={onChange}
        />
      </NextIntlClientProvider>,
    );

    expect(html.match(/API Base URLs/g)).toHaveLength(2);
    expect(html).toContain(
      "Octopus appends the operation path for each protocol.",
    );
    expect(html).toContain("Octopus does not add it automatically.");
    expect(html).toContain(
      'value="https://gateway.example/proxy?signature=a/+%2F&amp;token=x/"',
    );
    expect(html).toContain('value="https://versionless.example"');
    expect(onChange).not.toHaveBeenCalled();
  });
});
