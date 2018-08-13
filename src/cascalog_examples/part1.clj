(ns cascalog-examples.part1
  (:use [cascalog-examples.data])
  (:use [cascalog.api])
  (:require [cascalog.logic.ops :as c]))

;; I've commented the entire source to allow
;; the user to execute one query at a time
;; in your REPL.
(comment


;;
;; simple projection of fields and of course
;; you can change the disposition and
;; the number of fields in the projection side
;;
;; SQL:
;; SELECT name, user, age, country, active
;;   FROM USERS;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; DUMP users;
;;

(?<- (stdout)
     [?name ?user ?age ?country ?active]
     (USERS ?name ?user ?age ?country ?active))



;;
;; In reality the above query is split in to parts:
;; one is the actual query the second part is the
;; command to execute it and tell Cascalog where
;; to put the output.
;;
;; 1) query
;;    (<- [?name ?user ?age ?country ?active]
;;     (USERS ?name ?user ?age ?country ?active))
;;
;; 2) execution
;;    (?- (output) query)
;;
;; for this reason we encaplulate the execution
;; into a clojure function 'run<-'
;; which we'll use to run our queries.
(defn run<- [query]
  (?- (stdout) query))


;;
;; This is come comparable to the SQL statement
;;
(run<-
 (<- [?name ?user ?age ?country ?active]
     (USERS ?name ?user ?age ?country ?active)))


;;
;; In this case we apply a simple filter on a field
;; we want to see all active users
;;
;; SQL:
;; SELECT name, user, age, country, active
;;   FROM USERS
;;  WHERE active = true;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; out = FILTER users BY active == true;
;; DUMP out;
;;

(run<-
 (<-  [?name ?user ?age ?country ?active]
      (USERS ?name ?user ?age ?country ?active)
      (= ?active true)))


;;
;; Now we can see how easy is to add multiple filters
;;
;; SQL:
;; SELECT name, user, age, country, active
;;   FROM USERS
;;  WHERE active = true
;;    AND country = "India";
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; out = FILTER users BY active == true AND country == 'India';
;; DUMP out;
;;
(run<-
 (<-
  [?name ?user ?age ?country ?active]
  (USERS ?name ?user ?age ?country ?active)
  (= ?active true)
  (= ?country "India")))


;;
;; Expressing OR conditions is a bit more
;; complicated. In Clojure #'or is a macro
;; not a function, so is not composable as function,
;; and need to be wrapped.
;;
;; SQL:
;; SELECT name, user, age, country, active
;;   FROM USERS
;;  WHERE active = true
;;     OR age >= 70;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; out = FILTER users BY active == true OR age >= 70;
;; DUMP out;
;;
(deffilterfn active-or-senior [active age]
  (or active
      (>= age 70)))

(run<-
 (<-
  [?name ?user ?age ?country ?active]
  (USERS ?name ?user ?age ?country ?active)
  (active-or-senior :< ?active ?age)))

;;
;; The optional :< keyword denotes
;; input fields to the function
;; the ouput can be then placed into field
;; with :>
;; for example if we want to perform a simple
;; tranformation on a field, such as making
;; the username uppercase we can run
;;
;; SQL:
;; SELECT name, UPPER(user), age, country, active
;;   FROM USERS;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; out = FOREACH users GENERATE name, UPPER(user), age, country, active;
;; DUMP out;
;;
(run<-
 (<-
  [?name ?user2 ?age ?country ?active]
  (USERS ?name ?user ?age ?country ?active)
  (clojure.string/upper-case :< ?user :> ?user2)))

;; of course you can map over any custom clojure function,
;; but in this case you will have to use one of the special def
;; like 'defmapfn'. The def* macros from Cascalog they add a bit
;; of info into the the var so that cascalog will know how
;; to distribute your custom function in the jar for hadoop

(defmapfn my-upper [s]
  (clojure.string/upper-case s))

(run<-
 (<-
  [?name ?user2 ?age ?country ?active]
  (USERS ?name ?user ?age ?country ?active)
  (my-upper :< ?user :> ?user2)))


;;
;; ## AGGREGATIONS
;;

;;
;; Some simple aggregations over the entire
;; dataset
;;
;; SQL:
;; SELECT count(*), avg(age)
;;   FROM USERS;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; g = GROUP users ALL;
;; out = FOREACH g GENERATE COUNT(users), AVG(users.age);
;; DUMP out;
;;
(run<-
 (<-  [?count ?average]
      (USERS ?name ?user ?age ?country ?active)
      (c/count :> ?count)
      (c/avg :< ?age :> ?average)))


;;
;; Now aggregation over a group
;;
;; SQL:
;; SELECT count(*), avg(age), country
;;   FROM USERS
;;  GROUP BY country;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; g = GROUP users BY country;
;; out = FOREACH g GENERATE COUNT(users), AVG(users.age), group;
;; DUMP out;
;;
;; NOTE: this is unintuive.
;; In this case we just added a field in the projection
;; and nothing else and magically the aggregation is now
;; over a group. Where is the GROUP BY equivalent clause?
;;
(run<-
 (<-  [?count ?average ?country]
      (USERS ?name ?user ?age ?country ?active)
      (c/count :> ?count)
      (c/avg :< ?age :> ?average)))


;;
;; Let's find the number of active users by country
;;
;; SQL:
;; SELECT count(*), country
;;   FROM USERS
;;  WHERE active = true
;;  GROUP BY country;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; a = FILTER users BY active == true;
;; g = GROUP a BY country;
;; out = FOREACH g GENERATE COUNT(a), group;
;; DUMP out;
;;

(run<-
 (<-     [?count ?country]
         (USERS ?name ?user ?age ?country ?active)
         (= ?active true)
         (c/count :> ?count)))



;;
;; If you want to find the number of active users by country
;; and display only those who have 25 or more active users
;;
;; SQL:
;; SELECT count(*) as count, country
;;   FROM USERS
;;  WHERE active = true
;;  GROUP BY country
;; HAVING count >= 25;
;;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; a = FILTER users BY active == true;
;; g = GROUP a BY country;
;; f = FOREACH g GENERATE COUNT(a) as count, group as country;
;; out = FILTER f BY count >= 25;
;; DUMP out;
;;
(run<-
 (<-     [?count ?country]
         (USERS ?name ?user ?age ?country ?active)
         (= ?active true)
         (c/count :> ?count)
         (>= ?count 25)))


;;
;; to sort the result set we can use the :sort predicate
;;
;; SQL:
;; SELECT name, user, age, country, active
;;   FROM USERS
;;  ORDER BY age DESC;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; out = ORDER users BY age DESC;
;; DUMP out;
;;
;; global sorting is not very common in big data,
;; most of the times you will sort by group.
;; however if you have to sort the entire dataset
;; you have to write a function which takes
;; a query and a list of fields to sort by.
;;
(require '[cascalog.logic.vars :as v])
(import 'cascalog.ops.IdentityBuffer)

(defn global-sort [sq fields desc?]
  (let [out-fields (get-out-fields sq)
        new-out-fields (v/gen-nullable-vars (count out-fields))]
    (<- new-out-fields
        (sq :>> out-fields)
        (:sort :<< fields)
        (:reverse desc?)
        ((IdentityBuffer.) :<< out-fields :>> new-out-fields))))

(run<-
 (global-sort
  (<- [?name ?user ?age ?country ?active]
      (USERS ?name ?user ?age ?country ?active))
  ["?age"] true))


;;
;; Here we want to find the two oldest active user per country
;;
;; To do this in SQL we need to do some serious SQL kung-fu!!
;; Frankly it's unreadable, none could easily say what this
;; SQL query does.
;;
;;     SELECT u.name, u.user, u.age, u.country
;;      FROM USERS as u
;; LEFT JOIN USERS as u2
;;        ON u.country = u2.country
;;       AND u.age <= u2.age
;;     WHERE  u.active = true
;;       AND u2.active = true
;;  GROUP BY u.country, u.user
;;    HAVING count(*) <= 2;
;;
;; Top N by group is a rather complicate matter in SQL,
;; here you can find some alternative approaches
;; http://www.xaprb.com/blog/2006/12/07/how-to-select-the-firstleastmax-row-per-group-in-sql/
;;
;; The Pig version is a bit more intuitive:
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; a = FILTER users BY active == true;
;; g = GROUP a BY country;
;; out = FOREACH g {
;;    s = ORDER a by age DESC;
;;    l = LIMIT s 2;
;;    GENERATE FLATTEN(l);
;; }
;; DUMP out;
;;
;;
;; However in Cascalog it's very easy:
;; you make a projection by country (which creates a group)
;; and in the group you sort by age, reverse
;; and take the top 2
(run<-
 (<-    [?name1 ?user1 ?age1 ?country]
        (USERS ?name ?user ?age ?country ?active)
        (= ?active true)
        (:sort ?age)
        (:reverse true)
        ((c/limit 2) ?name ?user ?age :> ?name1 ?user1 ?age1)))



;;
;; ## JOINS
;;


;;
;; If we want to display the real name of each user together
;; with the game they played and their score we will need to:
;;
;; SQL:
;;     SELECT u.name, s.game, s.score
;;      FROM USERS as u, SCORES as s
;;     WHERE u.user = s.user;
;;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; scores = LOAD './data/scores.tsv' USING PigStorage()
;;         AS (user:chararray, game:chararray, score:int);
;; j = JOIN users BY user, scores by user;
;; out = FOREACH j GENERATE users::name, scores::game, scores::score;
;; DUMP out;
;;
;;
;; In order to join multiple datasources you need just to add their tap
;; and by mentioning the same field name on multiple sources
;; Cascalog will automatically infer the join key.
;;
(run<-
 (<-    [?name ?game ?score]
        (USERS  ?name ?user ?age ?country ?active)
        (SCORES ?user ?game ?score)))


;;
;; If we want to display all user which are younger than 15
;; and have more than 6000 points playing Tetris we will need to:
;;
;;     SELECT s.score, u.name, u.age, u.country
;;       FROM USERS u, SCORES s
;;      WHERE u.user = s.user
;;        AND s.game = "Tetris"
;;        AND u.age < 15
;;        AND s.score > 6000;
;;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; scores = LOAD './data/scores.tsv' USING PigStorage()
;;         AS (user:chararray, game:chararray, score:int);
;; fu = FILTER users BY age < 15;
;; fs = FILTER scores BY game == 'Tetris' AND score > 6000;
;; j = JOIN fu BY user, fs by user;
;; out = FOREACH j GENERATE fs::score, fu::name, fu::age, fu::country;
;; DUMP out;
;;
;; In order to join multiple datasources you need just to add their tap
;; and by mentioning the same field name on multiple sources
;; Cascalog will automatically infer the join key.
;;
(run<-
 (<-    [?score ?name ?age ?country]
        (USERS  ?name ?user ?age ?country ?active)
        (SCORES ?user ?game ?score)
        (= ?game "Tetris")
        (< ?age 15)
        (> ?score 6000)))



;;
;; If we want to find the who scored the highest points
;; at the Tetris game we will need to:
;;
;; SQL:
;; SELECT s.score, u.name, u.age, u.country
;;   FROM USERS u, SCORES s
;;  WHERE u.user = s.user
;;    AND s.game = 'Tetris'
;;    AND s.score = ( SELECT max(score) FROM SCORES WHERE game = 'Tetris' );
;;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; scores = LOAD './data/scores.tsv' USING PigStorage()
;;         AS (user:chararray, game:chararray, score:int);
;; fs = FILTER scores BY game == 'Tetris';
;; gfs = GROUP fs ALL;
;; maxscore = FOREACH gfs GENERATE MAX(fs.score);
;; j = JOIN users BY user, fs by user;
;; j2 = JOIN j BY score, maxscore BY $0;
;; out = FOREACH j2 GENERATE j::fs::score, j::users::name, j::users::age, j::users::country;
;; DUMP out;

(defn max-game-score [game]
  (<- [?mscore]
      (SCORES _ ?game ?score)
      (= game ?game)
      (c/max ?score :> ?mscore)))

(run<-
 (max-game-score "Tetris"))

(run<-
 (<- [?score ?name ?age ?country]
     (USERS ?name ?user ?age ?country _)
     (SCORES ?user ?game ?score)
     (= ?game "Tetris")
     ((max-game-score "Tetris") ?score)))

;;
;; As we seen from the previous example is very easy to define sub query and reuse them.
;; In this example I want to find the top 3 scorer per game.
;; This type of queries is very common for the analycts folks (top-n by group)
;;
;; SQL:
;;    SELECT s.game, s.score, u.name, u.age, u.country
;;      FROM SCORES as s
;; LEFT JOIN SCORES as s2
;;        ON s.game = s2.game
;;       AND s.score <= s2.score
;;      JOIN USERS as u
;;        ON s.user = u.user
;;  GROUP BY s.game, s.user
;;    HAVING count(*) <=3;
;;
;;
;; PIG:
;; users = LOAD './data/users.tsv' USING PigStorage()
;;         AS (name:chararray, user:chararray, age:int,
;;             country:chararray, active:boolean);
;; scores = LOAD './data/scores.tsv' USING PigStorage()
;;         AS (user:chararray, game:chararray, score:int);
;; j = JOIN users BY user, scores by user;
;; g = GROUP j BY game;
;; t = FOREACH g {
;;    s = ORDER j by score DESC;
;;    l = LIMIT s 3;
;;    GENERATE FLATTEN(l);
;; }
;; out = FOREACH t GENERATE game, score, name, age, country;
;; DUMP out;
;;
(run<-
 (<- [?game ?score1 ?name1 ?age1 ?country1]
     (SCORES ?user ?game ?score)
     (USERS ?name ?user ?age ?country _)
     (:sort ?score)
     (:reverse true)
     ((c/limit 3) :< ?name ?score ?age ?country :> ?name1 ?score1 ?age1 ?country1)))



;; end of comment block
)
