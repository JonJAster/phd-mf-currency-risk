using DataFrames
using CSV
using Dates

const FIELD_FOLDERS = [
    "info", "local-monthly-gross-returns", "local-monthly-net-returns", "monthly-costs",
    "monthly-morningstar-category", "monthly-net-assets", "usd-monthly-gross-returns",
    "usd-monthly-net-returns"
]

const COUNTRY_GROUPS = Dict(
    "lux" => ["Luxembourg"],
    "kor" => ["South Korea"],
    "usa" => ["United States"],
    "can-chn-jpn" => ["Canada", "China", "Japan"],
    "irl-bra" => ["Ireland", "Brazil"],
    "gbr-fra-ind" => ["United Kingdom", "France", "India"],
    "esp-tha-aus-zaf-mex-aut-che" => [
        "Spain", "Thailand", "Australia", "South Africa",
        "Mexico", "Austria", "Switzerland"
    ]
)

const INPUT_FILESTRING_BASE = "./data/raw/mutual-funds"
const OUTPUT_FILESTRING_BASE = "./data/prepared/mutual-funds"
const DATESTRING = r"\d{4}-\d{2}"

const EXPLICIT_TYPES = Dict(:FundId => String15, :SecId => String15)
const DATA_COLS = Not([:name, :fundid, :secid])

function load_file_by_parts(folder)
    folderstring = joinpath(INPUT_FILESTRING_BASE, folder)
    files = readdir(folderstring)
    data_parts = DataFrame[]

    for file in files
        filestring = joinpath(folderstring, file)
        if folder == "monthly-net-assets"
            read_data = read_without_thousands_separators(filestring)
        else
            read_data = CSV.read(
                filestring, DataFrame, types=EXPLICIT_TYPES, stringtype=String,
                truestrings=["Yes"], falsestrings=["No"]
            )
        end
        
        if folder == "info"
            normalise_headings!(read_data)
        else
            start_date = match(DATESTRING, names(read_data)[4]).match
            number_of_other_dates = size(read_data, 2) - 4
            data_dates = [Date(start_date) + Month(i) for i in 0:number_of_other_dates]
            
            column_names = Symbol.(
                [lowercase.(names(read_data)[1:3]); Dates.format.(data_dates, "yyyy-mm")]
            )
            rename!(read_data, column_names)
            global before = read_data
            read_data = (drop_missing_ids ∘ drop_empty_rows)(read_data)
            global after = read_data
            sum(ismissing.(before.fundid)) > 0 && error("stop and read")
        end

        push!(data_parts, read_data)
    end

    data = vcat(data_parts...) |> drop_empty_cols
    return data
end

function read_without_thousands_separators(filestring)
    open(filestring, "r") do file
        datastring = read(file, String)

        datastring = remove_thousand_separating_commas(datastring)

        read_data = CSV.read(
            IOBuffer(datastring), DataFrame, types=EXPLICIT_TYPES
        )

        return read_data
    end
end

remove_thousand_separating_commas(string) = replace(string, r",(?=\d{3}(,\d{3})*\")" => "")

function drop_missing_ids(df)
    fundid_not_empty = df.fundid .!= ""
    secid_not_empty = df.secid .!= ""
    no_empty_ids = df[fundid_not_empty .& secid_not_empty, :]
    no_missing_ids = dropmissing(no_empty_ids, [:fundid, :secid])
    return no_missing_ids
end

function drop_empty_rows(df)
    where_data_exists = .!ismissing.(df[!, DATA_COLS])
    any_data = reduce(.|, eachcol(where_data_exists))
    return df[any_data, :]
end

function drop_empty_cols(df)
    where_data_exists = .!ismissing.(df)
    any_data = any.(eachcol(where_data_exists))
    return df[:, any_data]
end

function normalise_headings!(data_part)
    raw_names = names(data_part)
    re_whitespace_plus_bracket = r"\s+\("
    re_close_bracket = r"\)"
    re_remaining_whitespace = r"\s+"

    underscored_names = replace.(raw_names, re_whitespace_plus_bracket => "_")
    bracket_stripped_names = replace.(underscored_names, re_close_bracket => "")
    dashed_names = replace.(bracket_stripped_names, re_remaining_whitespace => "-")

    rename!(data_part, lowercase.(dashed_names))
end

function map_country_to_group(country::AbstractString)
    for (group, countries) in COUNTRY_GROUPS
        if country in countries
            return group
        end
    end

    return "other"
end
map_country_to_group(::Missing) = return "other"

function save_file_by_country_group(data, folder, group_map)
    for group in [keys(COUNTRY_GROUPS)..., "other"]
        # Split the dataframe into a subset of rows with the given country group
        data_split = data[map(secid -> get(group_map, secid, ""),
                          data[!, :secid]) .== group, :]

        filestring = joinpath(OUTPUT_FILESTRING_BASE, folder, "mf_$(folder)_$group.csv")

        if !isdir(dirname(filestring))
            mkpath(dirname(filestring))
        end

        CSV.write(filestring, data_split)
    end
end

function regroup_data(folder, secid_to_group, info)
    folder_time_start = time()
    if folder == "info"
        data = info
    else
        data = load_file_by_parts(folder)
    end

    save_file_by_country_group(data, folder, secid_to_group)
        
    folder_duration = round(time() - folder_time_start, digits=2)
    println("Processed folder $folder in $folder_duration seconds")
end

function main()
    main_time_start = time()
    # Load info data first to build a map from fundid to domicile country group
    info = load_file_by_parts("info")

    info[!, :country_group] .= map_country_to_group.(info[!, :domicile])
    secid_to_group = Dict(zip(info[!, :secid], info[!, :country_group]))
    println("Regrouping files...")

    for folder in FIELD_FOLDERS
        regroup_data(folder, secid_to_group, info)
    end

    main_duration = round(time() - main_time_start, digits=2)
    println("Finished refining mutual fund data in $main_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

# sum(ismissing.(before.secid))
# sum(ismissing.(after.fundid))

# Debugging code

# for group in union(keys(COUNTRY_GROUPS), ["other"])
#     println("Checking $group")
#     dataset = DataFrame[]
#     for folder in FIELD_FOLDERS
#         timestart = time()
#         filestring = joinpath("./data/prepared/mutual-funds", folder, "mf_$(folder)_$group.csv")
#         df = CSV.read(filestring, DataFrame)
#         push!(dataset, df)
#     end

#     total_missing = 0

#     for (i,df) in enumerate(dataset)
#         num_missing = sum(ismissing, df[!, :fundid])
#         total_missing += num_missing
#         num_missing > 0 && println("missing fundids in df$i: $num_missing")
#     end

#     println("\ntotal missing: $total_missing\n")
# end

# This testing code loads all data for FIELD_FOLDERS[3] and checks for missing secids where the domicile is "irl-bra"
# folderstring = joinpath("./data/raw/mutual-funds", FIELD_FOLDERS[3])
# files = readdir(folderstring)
# missing_secid_rows = []
# info = load_file_by_parts("info")
# for file in files
#     filestring = joinpath(folderstring, file)
#     println(filestring)
#     df = CSV.read(filestring, DataFrame)
#     function correct_domicile(secid)
#         ismissing(secid) && return false
#         secid_lookup = info[info[!, :secid] .== secid,:]
#         size(secid_lookup, 1) == 0 && return false
#         return first(secid_lookup[!, :domicile]) == "irl-bra"
#     end
#     push!(missing_secid_rows, df[correct_domicile.(df.SecId), :])
# end

# Identify the first row with a missing fundid in the irl-bra dataframe for FIELD_FOLDERS[3] in the prepared data
# folderstring = joinpath("./data/prepared/mutual-funds", FIELD_FOLDERS[3])
# filestring = joinpath(folderstring, "mf_$(FIELD_FOLDERS[3])_irl-bra.csv")

# df = CSV.read(filestring, DataFrame)

# first_missing_row = findfirst(ismissing, df[!, :fundid])
# row = df[first_missing_row, 1:5]
# println("First missing row: $row")
# debug_secid = row.secid

# # Find the raw file for FIELD_FOLDERS[3] that contains the secid for the first missing row\
# folderstring = joinpath("./data/raw/mutual-funds", FIELD_FOLDERS[3])
# files = readdir(folderstring)
# debug_raw_file = DataFrame()
# for file in files
#     filestring = joinpath(folderstring, file)
#     df = CSV.read(filestring, DataFrame)
#     if any(df[!, :SecId] .== debug_secid)
#         println("Found in $filestring")
#         debug_raw_file = df
#         break
#     end
# end

# # Find the row
# debug_row = debug_raw_file[debug_raw_file[!, :SecId] .== debug_secid, 1:5]

# # It's missing in the raw file
# names(debug_raw_file)
# dropped = drop_missing_ids(debug_raw_file)

# function debug_prep(read_data)
#     start_date = match(DATESTRING, names(read_data)[4]).match
#     number_of_other_dates = size(read_data, 2) - 4
#     data_dates = [Date(start_date) + Month(i) for i in 0:number_of_other_dates]

#     column_names = Symbol.(
#         [lowercase.(names(read_data)[1:3]); Dates.format.(data_dates, "yyyy-mm")]
#     )
#     rename!(read_data, column_names)
#     return read_data
# end

# x = debug_prep(debug_raw_file)
# y = drop_missing_ids(x)

# y[y.secid .== debug_secid, 1:5]