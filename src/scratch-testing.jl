using DataFrames
using CSV
using Arrow
using Dates
using StatsBase

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    datafile = joinpath(DIRS.mf.refined, "mf-data.arrow")
    infofile = joinpath(DIRS.mf.init, "mf-info.arrow")

    data = Arrow.Table(datafile) |> DataFrame
    info = Arrow.Table(infofile) |> DataFrame
    
    function valuecount_print(df, col)
        m = countmap(df[!, col])
        # sort m by values
        sorted_m = sort(collect(m), by=x->x[2], rev=true)
        for (k,v) in sorted_m
            println("$k: $v")
        end

        total_nonmissing = sum(values(m)) - get(m, "", 0)
        total = nrow(df)
        println("$total_nonmissing non-missing values out of $total ($(round(total_nonmissing/total*100, digits=2))%)")
        return
    end

    println(names(info))

    valuecount_print(info, :global_category)
    valuecount_print(info, :morningstar_category)
    valuecount_print(info, :us_category_group)
    valuecount_print(info, :investment_area)

    println(describe(info))

    rawinfo = CSV.read(joinpath(DIRS.mf.raw, "info.csv"), DataFrame)

    println(countmap(rawinfo[!, "Global \nCategory"]))

    testinfo = init_raw(joinpath(DIRS.mf.raw, "info.csv"), info=true)

    println(describe(testinfo))

    valuecount_print(testinfo, :investment_area)
    testinfo[testinfo.us_category_group .== "",:]

    for x in Date(2023,1,1):Month(-1):Date(2020,1,1)
        println(x)
    end

    testdf = DataFrame(a=[1,2],b=[2,3])

    function f(df)
        df = df[df.a .== 2, :]
    end

    f(testdf)

    testdf

    testdf[testdf.a .== 2, :b]


end