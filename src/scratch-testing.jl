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
    datafile = joinpath(DIRS.mf.init, "mf-data.arrow")
    infofile = joinpath(DIRS.mf.init, "mf-info.arrow")

    data = Arrow.Table(datafile) |> DataFrame
    info = Arrow.Table(infofile) |> DataFrame

    for i in 1:20
        println("Row $i")
        for j in 1:ncol(info)
            println("$(names(info)[j]): $(info[i, j])")
        end
        println("---")
    end

    function test_to_file(searchkey)
        search_data = info[contains.(lowercase.(info.fund_legal_name), lowercase.(searchkey)),:]
        unique_data = unique(search_data, :fund_legal_name)
        CSV.write(joinpath(DIRS.test, "$searchkey.csv"), unique_data)
    end

    test_to_file("Wilshire")

    qlookup("FS0000AA6E")

    unique_names = unique(info, :fund_legal_name)

    CSV.write(joinpath(DIRS.test, "unique-names.csv"), unique_names)

    active_funds = Arrow.Table(joinpath(DIRS.test, "active-data.arrow")) |> DataFrame

    gb = groupby(active_funds, :secid)

    using DataFrames, Missings

    using DataFrames, Statistics

    # Sample DataFrame creation (Replace this with your actual DataFrame)
    # df = DataFrame(secid = ["A", "A", "A", "B", "B", "C", "C", "C"], date = [1,2,3,1,2,1,2,3], net_assets = [100, missing, 150, missing, 200, 300, missing, missing])

    # Function to analyze missing net_assets between first and last non-missing values
    function analyze_missing_net_assets(df::DataFrame)
        # Group by secid
        grouped = groupby(df, :secid)

        # Dictionary to hold the count of missing net_assets for each secid
        missing_counts = Dict()

        for group in grouped
            # Sort each group by date
            sort!(group, :date)

            # Find indices of the first and last non-missing net_assets values
            first_nonmissing = findfirst(!ismissing, group.net_assets)
            last_nonmissing = findlast(!ismissing, group.net_assets)

            # Check if both non-missing values exist
            if !isnothing(first_nonmissing) && !isnothing(last_nonmissing) && first_nonmissing < last_nonmissing
                # Calculate the number of missing values between the first and last non-missing values
                num_missing = count(ismissing, group.net_assets[first_nonmissing:last_nonmissing])

                if num_missing > 0
                    missing_counts[group[1, :secid]] = num_missing
                end
            end
        end

        # Calculate the number of unique secids with at least one missing value between the first and last non-missing net_assets
        num_unique_secids = length(keys(missing_counts))

        # Calculate the number of times each unique count of missing values occurs
        count_frequencies = countmap(values(missing_counts))

        return num_unique_secids, count_frequencies
    end

    # Example usage
    num_unique_secids, count_frequencies = analyze_missing_net_assets(active_funds)

    println(sort(count_frequencies))
    println(sum(values(count_frequencies)))

    missing_count = sum([(18+num)*count for (num, count) in count_frequencies])

    println(missing_count/nrow(active_funds))
    qhead(joinpath(DIRS.mf.init, "mf-data.arrow"))

    handtest_data = data[data.fundid .== unique(data.fundid)[51], :] # FS0000C44R
    CSV.write(joinpath(DIRS.test, "handtest.csv"), handtest_data)

    testdf = qlookup("FS0000C44R")

    testgb = groupby(testdf, :secid)

    

    println()

    println("Number of unique secids with missing net_assets between first and last non-missing values: $num_unique_secids")
    println("Frequency of each unique count of missing values: $count_frequencies")

end