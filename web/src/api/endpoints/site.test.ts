import { describe, expect, it } from "vitest";
import { BaseUrlMode } from "./channel";
import {
  deriveFollowSiteModelURL,
  followSiteModelEndpointConfig,
  getFollowSiteBaseURLChangeImpact,
  normalizeSiteModelEndpointConfig,
  resolveSiteDefaultEndpointSet,
  resolveSiteEndpointSet,
  type SiteModelEndpointConfig,
} from "./site";

describe("site model endpoint config", () => {
  it("preserves opaque query bytes while deriving FollowSite", () => {
    expect(
      deriveFollowSiteModelURL(
        " https://example.com///?signature=a/+%2F&token=x/&key=1&key=2&api_key=z%2B ",
      ),
    ).toBe(
      "https://example.com/v1?signature=a/+%2F&token=x/&key=1&key=2&api_key=z%2B",
    );
  });

  it("normalizes absent config to FollowSite", () => {
    expect(normalizeSiteModelEndpointConfig(null)).toEqual(
      followSiteModelEndpointConfig(),
    );
  });

  it("keeps weights only for weighted endpoint sets", () => {
    const config: SiteModelEndpointConfig = {
      default: {
        source: "custom",
        endpoint_set: {
          base_url_mode: BaseUrlMode.Random,
          base_urls: [{ url: "https://example.com/v1", weight: 9 }],
        },
      },
      route_overrides: [],
    };
    expect(normalizeSiteModelEndpointConfig(config).default).toEqual({
      source: "custom",
      endpoint_set: {
        base_url_mode: BaseUrlMode.Random,
        base_urls: [{ url: "https://example.com/v1" }],
      },
    });
  });

  it("keeps custom API base URLs exact instead of inferring version prefixes", () => {
    const baseURLs = [
      "https://versionless.example",
      "https://gateway.example/custom/prefix",
      "https://signed.example/proxy?signature=a/+%2F&token=x/",
    ];
    const config: SiteModelEndpointConfig = {
      default: {
        source: "custom",
        endpoint_set: {
          base_url_mode: BaseUrlMode.Failover,
          base_urls: baseURLs.map((url) => ({ url })),
        },
      },
      route_overrides: [],
    };

    const normalized = normalizeSiteModelEndpointConfig(config);

    expect(
      normalized.default.source === "custom"
        ? normalized.default.endpoint_set.base_urls.map(({ url }) => url)
        : [],
    ).toEqual(baseURLs);
  });

  it("uses a complete protocol override before the custom default", () => {
    const config: SiteModelEndpointConfig = {
      default: {
        source: "custom",
        endpoint_set: {
          base_url_mode: BaseUrlMode.Failover,
          base_urls: [{ url: "https://default.example/v1" }],
        },
      },
      route_overrides: [
        {
          route_type: "anthropic",
          endpoint_set: {
            base_url_mode: BaseUrlMode.Weighted,
            base_urls: [
              { url: "https://one.example/anthropic", weight: 3 },
              { url: "https://two.example/anthropic", weight: 1 },
            ],
          },
        },
      ],
    };
    const resolved = resolveSiteEndpointSet(
      config,
      "anthropic",
      "https://control.example",
    );
    expect(resolved.source).toBe("route_override");
    expect(resolved.endpoint_set).toEqual(config.route_overrides[0].endpoint_set);
    resolved.endpoint_set.base_urls[0].url = "mutated";
    expect(config.route_overrides[0].endpoint_set.base_urls[0].url).toBe(
      "https://one.example/anthropic",
    );
  });

  it("resolves the default independently from an override for the default protocol", () => {
    const config: SiteModelEndpointConfig = {
      default: { source: "follow_site" },
      route_overrides: [
        {
          route_type: "openai_chat",
          endpoint_set: {
            base_url_mode: BaseUrlMode.Failover,
            base_urls: [{ url: "https://override.example/v1" }],
          },
        },
      ],
    };

    expect(
      resolveSiteDefaultEndpointSet(config, "https://control.example")
        .endpoint_set,
    ).toEqual({
      base_url_mode: BaseUrlMode.Delay,
      base_urls: [{ url: "https://control.example/v1" }],
    });
  });

  it("reports only inherited protocols affected by a FollowSite base URL change", () => {
    const config: SiteModelEndpointConfig = {
      default: { source: "follow_site" },
      route_overrides: [
        {
          route_type: "anthropic",
          endpoint_set: {
            base_url_mode: BaseUrlMode.Failover,
            base_urls: [{ url: "https://override.example/anthropic" }],
          },
        },
      ],
    };
    const impacts = getFollowSiteBaseURLChangeImpact(
      config,
      "https://old.example",
      "https://new.example/",
      [
        {
          models: [
            { route_type: "anthropic" },
            { route_type: "gemini" },
            {},
          ],
        },
      ],
      "openai_chat",
    );
    expect(impacts).toEqual([
      {
        route_type: "gemini",
        previous_url: "https://old.example/v1",
        next_url: "https://new.example/v1",
      },
      {
        route_type: "openai_chat",
        previous_url: "https://old.example/v1",
        next_url: "https://new.example/v1",
      },
    ]);
    expect(
      getFollowSiteBaseURLChangeImpact(
        {
          default: {
            source: "custom",
            endpoint_set: {
              base_url_mode: BaseUrlMode.Delay,
              base_urls: [{ url: "https://custom.example/v1" }],
            },
          },
          route_overrides: [],
        },
        "https://old.example",
        "https://new.example",
        [{ models: [{ route_type: "gemini" }] }],
        "openai_chat",
      ),
    ).toEqual([]);
  });
});
