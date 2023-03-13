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

const INPUT_FILESTRING_BASE = "./Data/Raw Data/Mutual Funds"
const OUTPUT_FILESTRING_BASE = "./Data/Refined Data/Mutual Funds"
const DATESTRING = r"\d{4}-\d{2}"

function load_file_by_parts(folder)
    folderstring = joinpath(INPUT_FILESTRING_BASE, folder)
    files = readdir(folderstring)
    data = DataFrame()

    for file in files
        filestring = joinpath(folderstring, file)
        read_data = CSV.read(filestring, DataFrame)
        
        if folder == "info"
            raw_names = names(read_data)
            underscored_names = replace.(raw_names, r"\s+\(" => "_")
            bracket_stripped_names = replace.(underscored_names, r"\)" => "")
            dashed_names = replace.(bracket_stripped_names, r"\s+" => "-")
            rename!(read_data, lowercase.(dashed_names))
        else
            start_date::String7 = match(DATESTRING, names(read_data)[4]).match
            number_of_other_dates = size(read_data, 2) - 4
            data_dates = [Date(start_date) + Month(i) for i in 0:number_of_other_dates]

            rename!(
                read_data,
                Symbol.([
                    lowercase.(names(read_data)[1:3])...,
                    Dates.format.(data_dates, "yyyy-mm")...
                ])
            )
        end

        data = vcat(data, read_data)
    end

    return data
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
        data_split = data[map(secid -> get(group_map, String15(secid), ""),
                          data[!, :secid]) .== group, :]

        filestring = joinpath(OUTPUT_FILESTRING_BASE, folder, "mf_$(folder)_$group.csv")

        if !isdir(dirname(filestring))
            mkpath(dirname(filestring))
        end

        CSV.write(filestring, data_split)
    end
end

function main()
    # Load info data first to build a map from fundid to domicile country group
    info = load_file_by_parts("info")

    info[!, :country_group] .= map_country_to_group.(info[!, :domicile])
    secid_to_group = Dict(zip(info[!, :secid], info[!, :country_group]))

    for folder in FIELD_FOLDERS
        time_start = time()
        println("Processing folder: $folder")

        if folder == "info"
            data = info
        else
            data = load_file_by_parts(folder)
        end

        save_file_by_country_group(data, folder, secid_to_group)
        println("Finished in $(round(time() - time_start, digits=2)) seconds")
    end
end

main()