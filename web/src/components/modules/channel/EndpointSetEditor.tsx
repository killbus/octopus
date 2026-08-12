"use client";

import { Plus, X } from "lucide-react";
import { useTranslations } from "next-intl";
import { BaseUrlMode } from "@/api/endpoints/channel";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

export type EditableEndpoint = {
  url: string;
  weight?: number;
};

type EndpointURLListEditorProps<T extends EditableEndpoint> = {
  idPrefix: string;
  endpoints: T[];
  mode: BaseUrlMode;
  onChange: (endpoints: T[]) => void;
  createEndpoint: () => T;
};

export function EndpointURLListEditor<T extends EditableEndpoint>({
  idPrefix,
  endpoints,
  mode,
  onChange,
  createEndpoint,
}: EndpointURLListEditorProps<T>) {
  const t = useTranslations("channel.form");
  const items = endpoints.length > 0 ? endpoints : [createEndpoint()];

  const update = (index: number, patch: Partial<T>) => {
    onChange(items.map((item, current) => (current === index ? { ...item, ...patch } : item)));
  };

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-medium text-card-foreground">
          {t("baseUrls")} {items.length > 0 ? `(${items.length})` : ""}
        </label>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          onClick={() => onChange([...items, createEndpoint()])}
          className="h-6 px-2 text-xs text-muted-foreground/70 hover:text-muted-foreground hover:bg-transparent"
        >
          <Plus className="h-3 w-3 mr-1" />
          {t("add")}
        </Button>
      </div>
      <div className="space-y-2">
        {items.map((endpoint, index) => (
          <div key={`${idPrefix}-baseurl-${index}`} className="flex items-center gap-2">
            <Input
              id={`${idPrefix}-base-${index}`}
              type="url"
              value={endpoint.url}
              onChange={(event) => update(index, { url: event.target.value } as Partial<T>)}
              placeholder={t("baseUrlUrl")}
              required={index === 0}
              className="rounded-xl flex-1"
            />
            {mode === BaseUrlMode.Weighted ? (
              <Input
                type="number"
                min={1}
                value={endpoint.weight ?? 1}
                onChange={(event) => update(index, { weight: Number(event.target.value) || 1 } as Partial<T>)}
                placeholder={t("baseUrlWeight")}
                className="rounded-xl w-20"
                title={t("baseUrlWeight")}
              />
            ) : null}
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => onChange(items.filter((_, current) => current !== index))}
              disabled={items.length <= 1}
              className="h-8 w-8 p-0 rounded-xl text-muted-foreground hover:text-destructive disabled:opacity-40 hover:bg-transparent"
              title={t("remove")}
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        ))}
      </div>
    </div>
  );
}

type EndpointModeSelectProps = {
  idPrefix: string;
  value: BaseUrlMode;
  onChange: (mode: BaseUrlMode) => void;
};

export function EndpointModeSelect({ idPrefix, value, onChange }: EndpointModeSelectProps) {
  const t = useTranslations("channel.form");
  return (
    <div className="space-y-2">
      <label htmlFor={`${idPrefix}-base-url-mode`} className="text-sm font-medium text-card-foreground">
        {t("baseUrlMode")}
      </label>
      <Select value={String(value ?? BaseUrlMode.Delay)} onValueChange={(mode) => onChange(Number(mode) as BaseUrlMode)}>
        <SelectTrigger id={`${idPrefix}-base-url-mode`} className="rounded-xl w-full border border-border px-4 py-2 text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
          <SelectValue />
        </SelectTrigger>
        <SelectContent className="rounded-xl">
          <SelectItem className="rounded-xl" value={String(BaseUrlMode.Delay)}>{t("baseUrlModeDelay")}</SelectItem>
          <SelectItem className="rounded-xl" value={String(BaseUrlMode.Failover)}>{t("baseUrlModeFailover")}</SelectItem>
          <SelectItem className="rounded-xl" value={String(BaseUrlMode.Random)}>{t("baseUrlModeRandom")}</SelectItem>
          <SelectItem className="rounded-xl" value={String(BaseUrlMode.Weighted)}>{t("baseUrlModeWeighted")}</SelectItem>
        </SelectContent>
      </Select>
    </div>
  );
}
