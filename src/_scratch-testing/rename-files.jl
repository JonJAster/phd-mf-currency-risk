using FromFile

@from "../utils.jl" using ProjectUtilities

for folderpath in readdir(PATHS.rawfunds, join=true)
    for file in readdir(folderpath)
        if file[1] == 'm'
            part_number = match(r"(?<=part-)\d+", file).match
            mv(joinpath(folderpath, file), joinpath(folderpath, "part-$part_number.csv"))
        end
    end
end