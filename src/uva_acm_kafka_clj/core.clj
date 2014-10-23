(ns uva-acm-kafka-clj.core
  (:require [port-kafka.consumer :as c]
            [port-kafka.core :as pk]))


(def consumer-config {"zookeeper.connect" "localhost:2181"
                      "group.id" "uvaacm.demo"
                      "auto.offset.reset" "smallest"
                      "auto.commit.enable" "false"})

(defn home-handler [message consumer]
  (println "Home page Hit!" message))

(defn search-handler [message consumer]
  (println "Search executed!" message))

(defn product-handler [message consumer]
  (println "Someone viewed a product!" message))

(defn cart-handler [message consumer]
  (println "Product added to cart!" message))

(defn checkout-handler [message consumer]
  (println "Sale!" message))

(defn gone-handler [message consumer]
  (println "Exit!" message))

(def topic-config {"home" (int 1)
                   "search" (int 1)
                   "product" (int 1)
                   "cart" (int 1)
                   "checkout" (int 1)
                   "gone" (int 1)})

(defn run-consumer []
  (let [consumer (c/create consumer-config)
        topic-streams (c/create-topic-streams consumer topic-config)
        topic-handlers {:home {:handler home-handler
                               :buffer {:buffer-type :blocking
                                        :buffer-size 100}}
                        :search {:handler search-handler
                                 :buffer {:buffer-type :blocking
                                          :buffer-size 100}}
                        :product {:handler product-handler
                                  :buffer {:buffer-type :blocking
                                           :buffer-size 100}}
                        :cart {:handler cart-handler
                               :buffer {:buffer-type :blocking
                                        :buffer-size 100}}
                        :checkout {:handler checkout-handler
                                   :buffer {:buffer-type :blocking
                                            :buffer-size 100}}
                        :gone {:handler gone-handler
                               :buffer {:buffer-type :blocking
                                        :buffer-size 100}}}]
    (pk/consume! consumer topic-streams topic-handlers :json)))
