import z from "zod"
import { Effect, Layer, PubSub, ServiceMap, Stream } from "effect"
import { Log } from "../util/log"
import { Instance } from "../project/instance"
import { BusEvent } from "./bus-event"
import { GlobalBus } from "./global"
import { runCallbackInstance, runPromiseInstance } from "../effect/runtime"

export namespace Bus {
  const log = Log.create({ service: "bus" })

  export const InstanceDisposed = BusEvent.define(
    "server.instance.disposed",
    z.object({
      directory: z.string(),
    }),
  )

  // ---------------------------------------------------------------------------
  // Service definition
  // ---------------------------------------------------------------------------

  type Payload<D extends BusEvent.Definition = BusEvent.Definition> = {
    type: D["type"]
    properties: z.infer<D["properties"]>
  }

  export interface Interface {
    readonly publish: <D extends BusEvent.Definition>(
      def: D,
      properties: z.output<D["properties"]>,
    ) => Effect.Effect<void>
    readonly subscribe: <D extends BusEvent.Definition>(def: D) => Stream.Stream<Payload<D>>
    readonly subscribeAll: () => Stream.Stream<Payload>
  }

  export class Service extends ServiceMap.Service<Service, Interface>()("@opencode/Bus") {}

  export const layer = Layer.effect(
    Service,
    Effect.gen(function* () {
      const pubsubs = new Map<string, PubSub.PubSub<Payload>>()
      const wildcardPubSub = yield* PubSub.unbounded<Payload>()

      const getOrCreate = Effect.fnUntraced(function* (type: string) {
        let ps = pubsubs.get(type)
        if (!ps) {
          ps = yield* PubSub.unbounded<Payload>()
          pubsubs.set(type, ps)
        }
        return ps
      })

      function publish<D extends BusEvent.Definition>(
        def: D,
        properties: z.output<D["properties"]>,
      ) {
        return Effect.gen(function* () {
          const payload: Payload = { type: def.type, properties }
          log.info("publishing", { type: def.type })

          const ps = pubsubs.get(def.type)
          if (ps) yield* PubSub.publish(ps, payload)
          yield* PubSub.publish(wildcardPubSub, payload)

          GlobalBus.emit("event", {
            directory: Instance.directory,
            payload,
          })
        })
      }

      function subscribe<D extends BusEvent.Definition>(def: D): Stream.Stream<Payload<D>> {
        log.info("subscribing", { type: def.type })
        return Stream.unwrap(
          Effect.gen(function* () {
            const ps = yield* getOrCreate(def.type)
            return Stream.fromPubSub(ps) as Stream.Stream<Payload<D>>
          }),
        ).pipe(Stream.ensuring(Effect.sync(() => log.info("unsubscribing", { type: def.type }))))
      }

      function subscribeAll(): Stream.Stream<Payload> {
        log.info("subscribing", { type: "*" })
        return Stream.fromPubSub(wildcardPubSub).pipe(
          Stream.ensuring(Effect.sync(() => log.info("unsubscribing", { type: "*" }))),
        )
      }

      return Service.of({ publish, subscribe, subscribeAll })
    }),
  )

  // ---------------------------------------------------------------------------
  // Legacy adapters — plain function API wrapping the Effect service
  // ---------------------------------------------------------------------------

  function runStream(stream: (svc: Interface) => Stream.Stream<Payload>, callback: (event: any) => void) {
    return runCallbackInstance(
      Service.use((svc) =>
        stream(svc).pipe(Stream.runForEach((msg) => Effect.sync(() => callback(msg)))),
      ),
    )
  }

  export function publish<D extends BusEvent.Definition>(def: D, properties: z.output<D["properties"]>) {
    return runPromiseInstance(Service.use((svc) => svc.publish(def, properties)))
  }

  export function subscribe<D extends BusEvent.Definition>(
    def: D,
    callback: (event: Payload<D>) => void,
  ) {
    return runStream((svc) => svc.subscribe(def), callback)
  }

  export function subscribeAll(callback: (event: any) => void) {
    return runStream((svc) => svc.subscribeAll(), callback)
  }
}
