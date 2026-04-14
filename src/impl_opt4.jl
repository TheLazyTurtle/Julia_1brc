using Base.Threads

const amountOfLines = 100_000_000
testLock = ReentrantLock()

mutable struct Measurement
  @atomic min::Float64
  @atomic max::Float64
  @atomic sum::Float64
  @atomic amountOfPoints::Int64
end

function loadData()
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
    station, value = efficientSplit(point, ';')

    if haskey(dataPoints, station)
      existingPoint = dataPoints[station]
      @atomic min(existingPoint.min, value)
      @atomic max(existingPoint.max, value)
      @atomic existingPoint.sum += value
      @atomic existingPoint.amountOfPoints += 1
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

function mergeResults(fullResult::Dict{String, Measurement}, partialResults::Dict{String, Measurement})
  global testLock

  for partialMeasurement in partialResults
    lock(testLock)
    if haskey(fullResult, partialMeasurement.first)
      existingPoint = fullResult[partialMeasurement.first]
      unlock(testLock)
      @atomic min(existingPoint.min, partialMeasurement.second.min)
      @atomic max(existingPoint.max, partialMeasurement.second.max)
      @atomic existingPoint.sum += partialMeasurement.second.sum
      @atomic existingPoint.amountOfPoints += partialMeasurement.second.amountOfPoints
    else
      push!(fullResult, partialMeasurement)
      unlock(testLock)
    end
  end
end

function __init__()
  GC.enable(false) # Disable GC, as we run for a short period of time anyways, and there is no point in dealing with the overhead

  # *4 seems to give the most optimal result when ran with 15 threads. 
  # It might have to do with that reading speed of the file is the limiting factor, so if you make the chunkSize too large, most threads spend their time doing waiting for data
  amountOfThreads = nthreads() * 4
  chunkSize = fld(amountOfLines, amountOfThreads)

  mergedResults = Dict{String, Measurement}()
  sizehint!(mergedResults, 10_000) # The challange said that there could at max be 10_000 stations

  open("./data/measurements_100m.txt") do io
    i = 1
    lines = Vector{String}(undef, amountOfLines)
    @sync for line in eachline(io)
      lines[i] = line
      i += 1

      if i % chunkSize == 0
        t = fld(i, chunkSize)
        lo = (t - 1) * chunkSize + 1
        hi = t == amountOfThreads ? amountOfLines : t * chunkSize

        dataView = @view lines[lo:hi]
        @spawn begin 
         dataPoints = processData(dataView)
         mergeResults(mergedResults, dataPoints)
        end
      end
    end
  end

  # printData(mergedResults)
end

__init__()
sleep(1)
