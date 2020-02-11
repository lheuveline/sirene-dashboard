import * as p from "@bokehjs/core/properties";
import { HTMLBox } from "@bokehjs/models/layouts/html_box";
import { VTKAxes } from "./vtkaxes";
import { VTKHTMLBoxView } from "./vtk_layout";
export declare class VTKPlotView extends VTKHTMLBoxView {
    model: VTKPlot;
    protected _setting: boolean;
    protected _orientationWidget: any;
    protected _widgetManager: any;
    protected _axes: any;
    protected _axes_initialized: boolean;
    connect_signals(): void;
    render(): void;
    after_layout(): void;
    _create_orientation_widget(): void;
    _render_axes_canvas(): void;
    _orientation_widget_visbility(visbility: boolean): void;
    _get_camera_state(): void;
    _set_camera_state(): void;
    _delete_axes(): void;
    _set_axes(): void;
    _plot(): void;
}
export declare namespace VTKPlot {
    type Attrs = p.AttrsOf<Props>;
    type Props = HTMLBox.Props & {
        axes: p.Property<VTKAxes>;
        camera: p.Property<any>;
        data: p.Property<string>;
        enable_keybindings: p.Property<boolean>;
        orientation_widget: p.Property<boolean>;
    };
}
export interface VTKPlot extends VTKPlot.Attrs {
}
export declare class VTKPlot extends HTMLBox {
    properties: VTKPlot.Props;
    renderer_el: any;
    outline: any;
    outline_actor: any;
    constructor(attrs?: Partial<VTKPlot.Attrs>);
    getActors(): [any];
    static __module__: string;
    static init_VTKPlot(): void;
}
