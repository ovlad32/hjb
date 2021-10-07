dbfile="hjb.db3"
rm "${dbfile}";
if [ ! -f "${dbfile}" ]; then
    go run . -mode=init
    go run . -mode=indexing -baseTag="mt-base"
    go run . -mode=discovery -baseTag="mt-base" -lakeTag="cra-lake" -anchorUniqueness=1 -anchorMinCount=3 -anchorMaxCount=4
    go run . -mode=aggregation -baseTag="mt-base" -baseRowMinPct=0
fi

