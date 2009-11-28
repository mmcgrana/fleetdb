(ns scratch.bench-util
  (:import (com.yourkit.api Controller ProfilingModes)))

(defn- nano-time []
  (System/nanoTime))

(defn timed [label f]
  (let [t-start (nano-time)
        res     (f)
        t-end   (nano-time)
        elap    (double (/ (- t-end t-start) 1000000000))]
   (println label elap)
   res))

(defn profiled [body]
  (let [profiler (Controller.)]
    (.startCPUProfiling profiler
      ProfilingModes/CPU_TRACING
      Controller/DEFAULT_FILTERS
      Controller/DEFAULT_WALLTIME_SPEC)
    (let [res (body)]
      (.captureSnapshot profiler ProfilingModes/SNAPSHOT_WITHOUT_HEAP)
      (.stopCPUProfiling profiler)
      res)))