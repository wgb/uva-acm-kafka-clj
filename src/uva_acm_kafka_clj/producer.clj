(ns uva-acm-kafka-clj.producer
  (:require [port-kafka.producer :as p]
            [port-kafka.messages :as m]
            [causatum.event-streams :as es]))

(def producer-config {"metadata.broker.list" "localhost:9092"
                      "serializer.class" "kafka.serializer.DefaultEncoder"
                      "partitioner.class" "kafka.producer.DefaultPartitioner"})

(def model {:graph {:home [{:home {:weight 3
                                   :delay [:constant 2]}
                            :search {:weight 1
                                     :delay [:constant 3]}
                            :gone {:weight 10}}]
                    :search [{:home {:weight 1
                                     :delay [:random 10]}
                              :product {:weight 5
                                        :delay [:constant 3]}
                              :search {:weight 3
                                       :delay [:constant 5]}
                              :gone {:weight 2
                                     :delay [:constant 2]}}]
                    :product [{:home {:weight 1
                                      :delay [:random 10]}
                               :search {:weight 3
                                        :delay [:constant 4]}
                               :cart {:weight 4
                                      :delay [:constant 5]}
                               :gone {:weight 6
                                      :delay [:constant 2]}}]
                    :cart [{:home {:weight 1
                                   :delay [:random 10]}
                            :product {:weight 2
                                      :delay [:constant 2]}
                            :gone {:weight 3
                                   :delay [:random 5]}}
                           {:checkout {}}]}
            :delay-ops {:constant (fn [rtime n] n)
                        :random (fn [rtime n] (rand n))}})

(defn event->msg [event]
  (m/create-message (name (:state event))
                    {:relative-event-time (:rtime event)}
                    :json))

(defn produce-n-events [n]
  (let [kafka-producer (p/create producer-config)
        kafka-messages (->> (es/event-stream model
                                             (map (fn [rtime]
                                                    {:state :home :rtime rtime})
                                                  (iterate inc 0)))
                            (take n)
                            (map event->msg))]
    (doseq [message kafka-messages]
      (p/send! kafka-producer message))))
