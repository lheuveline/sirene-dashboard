import { PanelHTMLBoxView } from "../layout";
export declare class VTKHTMLBoxView extends PanelHTMLBoxView {
    protected _vtk_container: HTMLDivElement;
    protected _vtk_renwin: any;
    render(): void;
    after_layout(): void;
    _remove_default_key_binding(): void;
}
