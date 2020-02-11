import * as p from "@bokehjs/core/properties";
import { HTMLBox } from "@bokehjs/models/layouts/html_box";
import { VTKHTMLBoxView } from "./vtk_layout";
import { VolumeType } from "./vtk_utils";
declare type InterpolationType = 'fast_linear' | 'linear' | 'nearest';
export declare class VTKVolumePlotView extends VTKHTMLBoxView {
    model: VTKVolumePlot;
    protected _controllerWidget: any;
    protected _vtk_image_data: any;
    connect_signals(): void;
    readonly volume: any;
    readonly image_actor_i: any;
    readonly image_actor_j: any;
    readonly image_actor_k: any;
    readonly shadow_selector: HTMLSelectElement;
    readonly edge_gradient_slider: HTMLInputElement;
    readonly sampling_slider: HTMLInputElement;
    readonly colormap_slector: HTMLSelectElement;
    _set_interpolation(interpolation: InterpolationType): void;
    render(): void;
    _connect_controls(): void;
    _plot_volume(): void;
    _plot_slices(): void;
    _set_volume_visbility(visibility: boolean): void;
    _set_slices_visbility(visibility: boolean): void;
}
export declare namespace VTKVolumePlot {
    type Attrs = p.AttrsOf<Props>;
    type Props = HTMLBox.Props & {
        data: p.Property<VolumeType>;
        shadow: p.Property<boolean>;
        sampling: p.Property<number>;
        edge_gradient: p.Property<number>;
        colormap: p.Property<string>;
        rescale: p.Property<boolean>;
        ambient: p.Property<number>;
        diffuse: p.Property<number>;
        specular: p.Property<number>;
        specular_power: p.Property<number>;
        slice_i: p.Property<number>;
        slice_j: p.Property<number>;
        slice_k: p.Property<number>;
        display_volume: p.Property<boolean>;
        display_slices: p.Property<boolean>;
        render_background: p.Property<string>;
        interpolation: p.Property<InterpolationType>;
    };
}
export interface VTKVolumePlot extends VTKVolumePlot.Attrs {
}
export declare class VTKVolumePlot extends HTMLBox {
    properties: VTKVolumePlot.Props;
    constructor(attrs?: Partial<VTKVolumePlot.Attrs>);
    static __module__: string;
    static init_VTKVolumePlot(): void;
}
export {};
