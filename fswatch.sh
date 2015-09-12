#!/bin/bash
# source me

fswatch -0 -e ".*" -i ".*clj$" \
	--event Updated \
	src/clojure/migae \
    | xargs -0 -n 1 -I {} sh -c 'gcp -v {} ~/norc.dev/sparky/gae/ear/build/exploded-app/sparky-0.5.1/WEB-INF/classes/migae/;' &

fswatch -0 -e ".*" -i ".*clj$" \
	--event Updated \
	src/clojure/migae/datastore \
    | xargs -0 -n 1 -I {} sh -c 'gcp -v {} ~/norc.dev/sparky/gae/ear/build/exploded-app/sparky-0.5.1/WEB-INF/classes/migae/datastore;' &

