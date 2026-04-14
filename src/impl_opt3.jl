using Base.Threads

const amountOfLines = 100_000_000

mutable struct Measurement
  min::Float64
  max::Float64
  sum::Float64
  amountOfPoints::Int64
end

function loadData()
  # TODO: Optimize this by reading as raw bytes and processing that into strings. This might save in allocations, as now the loading of data is about twice the size of the file
  lines = Vector{String}(undef, amountOfLines)
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
  @inbounds begin
    name = SubString(line, firstindex(line), prevind(line, i))
    # TODO: Parse is heavy, as it has to make a expensive c call. Try and fix
    val = parse(Float64, SubString(line, nextind(line, i), lastindex(line)))
    return name, val
  end
end

function processData(data)
  dataPoints = Dict{String, Measurement}()
  sizehint!(dataPoints, 10_000) # The challange said that there could at max be 10_000 stations

  @inbounds for point in data
    passedSeperator = false

    station, value = efficientSplit(point, ';')

    if haskey(dataPoints, station)
      existingPoint = dataPoints[station]
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
  ks = Vector{String}(undef, length(dataPoints))
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

  io = IOBuffer()

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

function mergeResults(results::Vector{Dict{String, Measurement}})
  resultDict = Dict{String, Measurement}()
  sizehint!(resultDict, 10_000) # The challange said that there could at max be 10_000 stations

  for r in results
    for key in keys(r)
      value = r[key]
      if haskey(resultDict, key)
        existingPoint = resultDict[key]
        if existingPoint.min > value.min
          existingPoint.min = value.min
        end
        if existingPoint.max < value.max
          existingPoint.max = value.max
        end
        existingPoint.sum += value.sum
        existingPoint.amountOfPoints += value.amountOfPoints
      else
        push!(resultDict, key => value)
      end
    end
  end

  return resultDict
end

function __init__()
  amountOfThreads = nthreads()
  # amountOfLines = 10_000_000

  data = loadData()
  chunkSize = fld(amountOfLines, amountOfThreads)

  results = Vector{Dict{String, Measurement}}(undef, amountOfThreads)

  @threads for t in 1:amountOfThreads
  # for t in 1:amountOfThreads
      lo = (t - 1) * chunkSize + 1
      hi = t == amountOfThreads ? amountOfLines : t * chunkSize

      dataView = @view data[lo:hi]
      dataPoints = processData(dataView)
      results[t] = dataPoints
  end

  mergedDataPoints = mergeResults(results)
  printData(mergedDataPoints)
end

__init__()
sleep(1)
