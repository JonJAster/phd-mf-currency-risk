using Arrow
using CSV
using DataFrames
using Dates
using StatsBase
using BenchmarkTools

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using .CommonFunctions
using .CommonConstants
using .DataInit

OUTPUT_DIR = "data/test-data"

function test()
    
    options_folder=option_foldername(; DEFAULT_OPTIONS...)
    path = joinpath(DIRS.fund, "post-processing", options_folder, "main/fund_data.arrow")
    path_factors = joinpath(DIRS.equity, "factor-series/global_equity_factors.arrow")
    path_curr = joinpath(DIRS.currency, "factor-series/currency_factors.arrow")

    ###

    path_info = joinpath(DIRS.fund, "domicile-grouped/info/mf_info_usa.csv")
    df_info = CSV.read(path_info, DatcaFrame)
    df_raw = load_raw("domicile-grouped", "usa")

    dropmissing!(df_raw, "local-monthly-gross-returns")

    gb_funds = groupby(df_raw, "fundid")
    tenures = combine(
        gb_funds,
        "date" => (x->length(unique(x))) => :tenure
    )

    tenlist = tenures[tenures.tenure .== 60, :]

    print(first(tenlist.fundid, 10))

    infolookup("FS0000A1MC")
    list_categories(df_raw, "FS0000A1MC"; markup=true)

    # CSV.write(joinpath(OUTPUT_DIR, "usa-fund-tenures.csv"), tenures)
    
    tenures[tenures.fundid .== "FS00009UF0", :]
    df_raw[df_raw.fundid .== "FS00009UF0", :]

    CSV.write(joinpath(OUTPUT_DIR, "borderline-mature-fund.csv"), df_raw[df_raw.fundid .== "FS0000A1MC", :])

    function list_categories(df, fundid; markup=false)
        fund_data = df[df.fundid .== fundid, ["secid", "monthly-morningstar-category"]]
        gb = groupby(fund_data, :secid)
        
        categories = combine(
            gb,
            "monthly-morningstar-category" => string_categories => :categories
        )

        if markup
            println("| "*join(names(categories), " | ")*" |")
            println("| "*join(fill("---", length(names(categories))), " | ")*" |")
            for row in eachrow(categories)
                println("| "*join(row, " | ")*" |")
            end
            return
        end

        return categories
    end

    function list_categories(df; secid, markup=false)
        fundid = df[df.secid .== secid, "fundid"] |> first
        sec_data = df[df.secid .== secid, "monthly-morningstar-category"]
        output = DataFrame(
            :fundid => fundid,
            :secid => secid,
            :categories => string_categories(sec_data)
        )

        if markup
            println("| "*join(names(categories), " | ")*" |")
            println("| "*join(fill("---", length(names(categories))), " | ")*" |")
            for row in eachrow(categories)
                println("| "*join(row, " | ")*" |")
            end
            return
        end

        return output
    end
    
    function string_categories(categories)
        map(x->ismissing(x) ? "missing" : x, categories)
        categories = unique(skipmissing(categories))
        category_string = first(categories)
        i = 2
        while i <= length(categories)
            category_string *= ", " * categories[i]
            i += 1
        end
        return category_string
    end

    gb_catcount = groupby(df_raw, :secid)
    catcount = combine(
        gb_catcount,
        "monthly-morningstar-category" => (x->length(unique(x))) => :catcount
    )
    sort(countmap(catcount.catcount))
    sort(Dict(n=>countmap(catcount.catcount)[n]/length(catcount.catcount) for n in 1:7))
    catcount[catcount.catcount .== 7, :]
    
    list_categories(df_raw, secid="FOUSA00CRI")
    
    ###

    df = Arrow.Table(path) |> DataFrame
    df_factors = Arrow.Table(path_factors) |> DataFrame
    df_curr = Arrow.Table(path_curr) |> DataFrame

    minimum(df.date)

    test_df = DataFrame(
        a = [missing, missing, missing, missing, missing, missing, missing],
        b = [1, missing, missing, missing, missing, missing, 7],
        c = [1, missing, missing, missing, missing, 6, missing],
        d = [1, missing, missing, missing, 5, 6, 7],
        e = [1, missing, missing, 4, 5, missing, missing],
        f = [1, missing, 3, 4, 5, missing, 7],
        g = [1, missing, 3, 4, missing, 6, missing],
        h = [1, missing, 3, missing, missing, missing, 7],
        i = [1, missing, 3, 3, missing, missing, missing],
        k = [1, missing, 3, missing, 5, 6, 7]
    )

    drop_allmissing!(test_df, dims=:rows)
    cols = [:a, :f, :g]
    cols[[1,3]]
    select(test_df, Not([1,2]))

    supertest_df = reduce(vcat, fill(test_df, 100_000))

    dropmissing(test_df, :k)

    delete!(test_df, findall(all_missing))

    # Benchmark this call
    all_missing = .!(Matrix(test_df) .|> ismissing) * ones(size(test_df, 2)) .== zeros(size(test_df, 1))
    @benchmark all(Matrix(test_df) .|> ismissing, dims=2)

    @benchmark (Matrix(supertest_df) .|> ismissing) * ones(size(supertest_df, 2)) .== zeros(size(supertest_df, 1))
    @benchmark .!any(Matrix(supertest_df) .|> ismissing, dims=2)
end