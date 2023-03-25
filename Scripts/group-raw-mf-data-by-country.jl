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

const INPUT_FILESTRING_BASE = "./Data/Refined Data/Mutual Funds/Cleaned Files"
const OUTPUT_FILESTRING_BASE = "./Data/Refined Data/Mutual Funds/Grouped Files"
const DATESTRING = r"\d{4}-\d{2}"

const EXPLICIT_TYPES = Dict("FundId"=>String15, "SecId"=>String15)

function load_file_by_parts(folder, debug_mode=false)
    read_limit = debug_mode ? 500 : nothing
    folderstring = joinpath(INPUT_FILESTRING_BASE, folder)
    files = readdir(folderstring)
    data = DataFrame()

    for file in files
        filestring = joinpath(folderstring, file)
        read_data = (
            CSV.read(filestring, DataFrame, limit=read_limit, types=EXPLICIT_TYPES,
                     stringtype=String, truestrings=["Yes"], falsestrings=["No"])
        )
        
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
        end

        data = vcat(data, read_data)
    end

    return data
end

function normalise_headings!(data_part)
    raw_names = names(data_part)
    re_whitespace_plus_bracket = r"[^\S\r\n]+\("
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

function main(debug_mode=false)
    main_time_start = time()
    # Load info data first to build a map from fundid to domicile country group
    info = load_file_by_parts("info")

    info[!, :country_group] .= map_country_to_group.(info[!, :domicile])
    secid_to_group = Dict(zip(info[!, :secid], info[!, :country_group]))
    println("Regrouping files...")

    for folder in FIELD_FOLDERS
        folder_time_start = time()
        
        if folder == "info"
            data = info
        else
            data = load_file_by_parts(folder, debug_mode)
        end

        save_file_by_country_group(data, folder, secid_to_group)
        folder_duration = round(time() - folder_time_start, digits=2)
        println("Processed folder $folder in $folder_duration seconds")
    end
    main_duration = round(time() - main_time_start, digits=2)
    println("Finished refining mutual fund data in $main_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end