mutable struct Measurement
  min::Float64
  max::Float64
  sum::Float64
  amountOfPoints::Int64
end

function loadData()
  lines = Vector{String}(undef, 100_000_000)
  open("./data/measurements_100m.txt") do io
    i = 1
    for line in eachline(io)
      lines[i] = line
      i += 1
    end
  end

  return lines
end

function efficientSplit(line::String, delim::Char)
  i = findfirst(==(delim), line)
  @inbounds return SubString(line, firstindex(line), prevind(line, i)), parse(Float64, SubString(line, nextind(line, i), lastindex(line)))
end

function processData(data)
  dataPoints = Dict{String, Measurement}()
  sizehint!(dataPoints, 10_000) # The challange said that there could at max be 10_000 stations

  for point in data
    passedSeperator = false

    station, value = efficientSplit(point, ';')

    if haskey(dataPoints, station)
      existingPoint = get(dataPoints, station, 0)
      if existingPoint.min > value
        existingPoint.min = value
      end
      if existingPoint.max < value
        existingPoint.max = value
      end
      existingPoint.sum += value
      existingPoint.amountOfPoints += 1
    else
      push!(dataPoints, station => Measurement(value, value, value, 1))
    end
  end

  return dataPoints
end

function sortData(dataPoints)
  # Copy the keys ourselves into a vector, to prevent having to grow the vector
  # If we would have just called collect, it would have had to grow the vector
  ks = Vector{String}(undef, length(dataPoints)) #TODO: Maybe hard code the length
  i = 1
  for k in keys(dataPoints)
    ks[i] = k
    i += 1
  end
  return sort!(ks, alg=QuickSort)
end

function printData(dataPoints)
  print("{")

  sortedKeys = sortData(dataPoints)

  for key in sortedKeys
    value = dataPoints[key]
    print(key)
    print("=")
    print(round(value.min, digits=1))
    print("/")
    print(round(value.sum/value.amountOfPoints, digits=1))
    print("/")
    print(round(value.max, digits=1))
    print(", ")
  end

  print("}")
end

function __init__()
  data = loadData()
  dataPoints = processData(data)
  printData(dataPoints)
end

__init__()
sleep(1)
