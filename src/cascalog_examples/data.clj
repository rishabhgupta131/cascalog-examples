(ns cascalog-examples.data)

(comment

  (defn pprint-to-file [file data]
    (spit file
          (let [w (java.io.StringWriter.)]
            (binding [*print-length* (max 1000 (count data))]
              (clojure.pprint/pprint data w)
              (.toString w)))))



  (defn export-tsv [file seq]
    (->> seq
         (map (partial clojure.string/join "\t"))
         (clojure.string/join "\n")
         (#(str % "\n"))
         (spit file) ))

  (export-tsv "./data/users.tsv" USERS)
  (export-tsv "./data/scores.tsv" SCORES)


)

;; users dataset
(def USERS  (read-string (slurp "./data/users.edn")))
;; scores dataset
(def SCORES (read-string (slurp "./data/scores.edn")))
