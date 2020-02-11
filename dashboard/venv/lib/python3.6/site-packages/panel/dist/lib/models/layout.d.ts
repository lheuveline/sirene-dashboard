import { Layoutable } from "@bokehjs/core/layout/layoutable";
import { Size, SizeHint } from "@bokehjs/core/layout/types";
import { MarkupView } from "@bokehjs/models/widgets/markup";
import { HTMLBox, HTMLBoxView } from "@bokehjs/models/layouts/html_box";
export declare function set_size(el: HTMLElement, model: HTMLBox): void;
export declare class CachedVariadicBox extends Layoutable {
    readonly el: HTMLElement;
    readonly sizing_mode: string | null;
    readonly changed: boolean;
    _cache: {
        [key: string]: Size;
    };
    _cache_count: {
        [key: string]: number;
    };
    constructor(el: HTMLElement, sizing_mode: string | null, changed: boolean);
    protected _measure(viewport: Size): SizeHint;
}
export declare class PanelMarkupView extends MarkupView {
    _prev_sizing_mode: string | null;
    _update_layout(): void;
    render(): void;
}
export declare class PanelHTMLBoxView extends HTMLBoxView {
    _prev_sizing_mode: string | null;
    _update_layout(): void;
    render(): void;
}
