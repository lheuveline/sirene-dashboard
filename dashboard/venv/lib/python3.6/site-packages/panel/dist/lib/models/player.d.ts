import * as p from "@bokehjs/core/properties";
import { Widget, WidgetView } from "@bokehjs/models/widgets/widget";
export declare class PlayerView extends WidgetView {
    model: Player;
    protected sliderEl: HTMLInputElement;
    protected loop_state: HTMLFormElement;
    protected timer: any;
    connect_signals(): void;
    get_height(): number;
    render(): void;
    set_frame(frame: number): void;
    get_loop_state(): string;
    set_loop_state(state: string): void;
    next_frame(): void;
    previous_frame(): void;
    first_frame(): void;
    last_frame(): void;
    slower(): void;
    faster(): void;
    anim_step_forward(): void;
    anim_step_reverse(): void;
    pause_animation(): void;
    play_animation(): void;
    reverse_animation(): void;
}
export declare namespace Player {
    type Attrs = p.AttrsOf<Props>;
    type Props = Widget.Props & {
        direction: p.Property<number>;
        interval: p.Property<number>;
        start: p.Property<number>;
        end: p.Property<number>;
        step: p.Property<number>;
        loop_policy: p.Property<any>;
        value: p.Property<any>;
    };
}
export interface Player extends Player.Attrs {
}
export declare abstract class Player extends Widget {
    properties: Player.Props;
    constructor(attrs?: Partial<Player.Attrs>);
    static __module__: string;
    static init_Player(): void;
}
