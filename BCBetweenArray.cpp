//
// Created by rony on 17. 3. 21.
//

//#define PROJECT_ROOT "@CMAKE_SOURCE_DIR@"

#include "BCBetweenArray.h"
#include <system/Exceptions.h>
#include <util/SpatialType.h>
#include <system/Utils.h>
#include <execinfo.h>
#include <dlfcn.h>
#include <cxxabi.h>

namespace scidb
{
    using namespace boost;

    static std::string Backtrace(int skip = 1)
    {
        void *callstack[128];
        const int nMaxFrames = sizeof(callstack) / sizeof(callstack[0]);
        char buf[1024];
        int nFrames = backtrace(callstack, nMaxFrames);
        char **symbols = backtrace_symbols(callstack, nFrames);

        std::ostringstream trace_buf;
        for (int i = skip; i < nFrames; i++) {
            Dl_info info;
            if (dladdr(callstack[i], &info)) {
                char *demangled = NULL;
                int status;
                demangled = abi::__cxa_demangle(info.dli_sname, NULL, 0, &status);
                snprintf(buf, sizeof(buf), "%-3d %*0p %s + %zd\n",
                         i, 2 + sizeof(void*) * 2, callstack[i],
                         status == 0 ? demangled : info.dli_sname,
                         (char *)callstack[i] - (char *)info.dli_saddr);
                free(demangled);
            } else {
                snprintf(buf, sizeof(buf), "%-3d %*0p\n",
                         i, 2 + sizeof(void*) * 2, callstack[i]);
            }
            trace_buf << buf;

            snprintf(buf, sizeof(buf), "%s\n", symbols[i]);
            trace_buf << buf;
        }
        free(symbols);
        if (nFrames == nMaxFrames)
            trace_buf << "[truncated]\n";
        return trace_buf.str();
    }

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.bcbetween"));
    /**
     * BCBetweenChunk method
     */
    std::shared_ptr<scidb::ConstChunkIterator>
    scidb::BCBetweenChunk::getConstIterator(int iterationMode) const
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunk::getConstIterator()");

        AttributeDesc const& attr = getAttributeDesc();
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;

        LOG4CXX_DEBUG(logger, "BCBetweenChunk::getConstIterator() -");

        return std::shared_ptr<ConstChunkIterator>(
            attr.isEmptyIndicator()
            ? (attrID >= array.getInputArray()->getArrayDesc().getAttributes().size())
                ? fullyInside
                    ? (ConstChunkIterator*)new EmptyBitmapBCBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                    : (ConstChunkIterator*)new NewBitmapBCBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                : fullyInside
                    ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                    : (ConstChunkIterator*)new ExistedBitmapBCBetweenChunkIterator(*this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
            : fullyInside
                ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode)
                : (ConstChunkIterator*)new BCBetweenChunkIterator(*this, iterationMode)
        );
    }

    void scidb::BCBetweenChunk::setInputChunk(const scidb::ConstChunk &inputChunk)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunk::setInputChunk()");

        DelegateChunk::setInputChunk(inputChunk);
        myRange._low = inputChunk.getFirstPosition(true);
        myRange._high = inputChunk.getLastPosition(true);

        // TO-DO: the fullyInside computation is simple but not optimal.
        // It is possible that the current chunk is fully inside the union of the specified ranges,
        // although not fully contained in any of them.
        size_t dummy = 0;
        fullyInside  =  array._spatialRangesPtr->findOneThatContains(myRange, dummy);
        fullyOutside = !array._spatialRangesPtr->findOneThatIntersects(myRange, dummy);

        isClone = fullyInside && attrID < array.getInputArray()->getArrayDesc().getAttributes().size();
        if (emptyBitmapIterator) {
            if (!emptyBitmapIterator->setPosition(inputChunk.getFirstPosition(false)))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
        LOG4CXX_DEBUG(logger, "BCBetweenChunk::setInputChunk() -");
    }

    scidb::BCBetweenChunk::BCBetweenChunk(const scidb::BCBetweenArray &arr,
                                          const scidb::DelegateArrayIterator &iterator,
                                          scidb::AttributeID attrID)
            : DelegateChunk(array, iterator, attrID, false),
              array(arr),
              myRange(arr.getArrayDesc().getDimensions().size()),
              fullyInside(false),
              fullyOutside(false)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunk::BCBetweenChunk()");

        tileMode = false;
    }

    Value const &BCBetweenChunkIterator::getItem()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::getItem()");

        if (!hasCurrent)
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::getItem() -");
        return inputIterator->getItem();
    }

    bool BCBetweenChunkIterator::isEmpty() const
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::isEmpty()");

        if (!hasCurrent)
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::isEmpty() -");
        return inputIterator->isEmpty() ||
               !array._spatialRangesPtr->findOneThatContains(currPos, _hintForSpatialRanges);
    }

    bool BCBetweenChunkIterator::end()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::end()");

        return !hasCurrent;
    }

    void BCBetweenChunkIterator::operator++()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::operator++()");

        if (_ignoreEmptyCells) {
            while (true) {
                ++(*inputIterator);
                if (!inputIterator->end()) {
                    Coordinates const& pos = inputIterator->getPosition();

                    if (array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                        currPos = pos;
                        hasCurrent = true;

                        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::operator++() array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)");
                        return;
                    }
                } else {
                    break;
                }
            }
            hasCurrent = false;
        } else {
            ++(*inputIterator);
            hasCurrent = !inputIterator->end();
        }

        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::operator++() -");
    }

    Coordinates const &BCBetweenChunkIterator::getPosition()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::getPosition()");

        return _ignoreEmptyCells ? currPos : inputIterator->getPosition();
    }

    bool BCBetweenChunkIterator::setPosition(Coordinates const &pos)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::setPosition()");

        if (_ignoreEmptyCells) {
            if (array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                hasCurrent = inputIterator->setPosition(pos);
                if (hasCurrent) {
                    currPos = pos;
                }
                return hasCurrent;
            }
            hasCurrent = false;
            return false;
        }

        hasCurrent = inputIterator->setPosition(pos);

        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::setPosition() -");
        return hasCurrent;
    }

    void BCBetweenChunkIterator::reset()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::reset()");

        if ( _ignoreEmptyCells ) {
            inputIterator->reset();
            if (!inputIterator->end()) {
                Coordinates const& pos = inputIterator->getPosition();
                if (array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                    currPos = pos;
                    hasCurrent = true;
                    return;
                }
                else {
                    ++(*this);  // The operator++() will skip the cells outside all the requested ranges.
                }
            }
            else {
                hasCurrent = false;
            }
        } else {
            inputIterator->reset();
            hasCurrent = !inputIterator->end();
        }

        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::reset() -");
    }

    ConstChunk const &BCBetweenChunkIterator::getChunk()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::getChunk()");

        return chunk;
    }

    BCBetweenChunkIterator::BCBetweenChunkIterator(BCBetweenChunk const &chunk, int iterationMode)
    : CoordinatesMapper(chunk),
      array(chunk.array),
      chunk(chunk),
      inputIterator(chunk.getInputChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
      currPos(array.getArrayDesc().getDimensions().size()),
      _mode(iterationMode & ~INTENDED_TILE_MODE & ~TILE_MODE),
      _ignoreEmptyCells((iterationMode & IGNORE_EMPTY_CELLS) == IGNORE_EMPTY_CELLS),
      type(chunk.getAttributeDesc().getType()),
      _hintForSpatialRanges(0)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::BCBetweenChunkIterator()");

        reset();

        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::BCBetweenChunkIterator() -");
    }

    // Existed Bitmap chunk iterator methods
    Value const &ExistedBitmapBCBetweenChunkIterator::getItem()
    {
        LOG4CXX_DEBUG(logger, "ExistedBitmapBCBetweenChunkIterator::getItem()");

        _value.setBool(
                inputIterator->getItem().getBool() &&
                array._spatialRangesPtr->findOneThatContains(currPos, _hintForSpatialRanges));

        LOG4CXX_DEBUG(logger, "ExistedBitmapBCBetweenChunkIterator::getItem() -");
        return _value;
    }

    ExistedBitmapBCBetweenChunkIterator::ExistedBitmapBCBetweenChunkIterator(
            BCBetweenChunk const &chunk, int iterationMode)
            : BCBetweenChunkIterator(chunk, iterationMode),
              _value(TypeLibrary::getType(TID_BOOL))
    {
        LOG4CXX_DEBUG(logger, "ExistedBitmapBCBetweenChunkIterator::ExistedBitmapBCBetweenChunkIterator()");
    }

    // New bitmap chunk iterator methods
    Value const &NewBitmapBCBetweenChunkIterator::getItem()
    {
        LOG4CXX_DEBUG(logger, "NewBitmapBCBetweenChunkIterator::getItem()");

        _value.setBool(array._spatialRangesPtr->findOneThatContains(currPos, _hintForSpatialRanges));

        LOG4CXX_DEBUG(logger, "NewBitmapBCBetweenChunkIterator::getItem() -");
        return _value;
    }

    NewBitmapBCBetweenChunkIterator::NewBitmapBCBetweenChunkIterator(
            BCBetweenChunk const &chunk, int iterationMode)
            : BCBetweenChunkIterator(chunk, iterationMode),
              _value(TypeLibrary::getType(TID_BOOL))
    {
        LOG4CXX_DEBUG(logger, "NewBitmapBCBetweenChunkIterator::NewBitmapBCBetweenChunkIterator()");
    }

    // Empty bitmap chunk iterator methods
    Value const &EmptyBitmapBCBetweenChunkIterator::getItem()
    {
        LOG4CXX_DEBUG(logger, "EmptyBitmapBCBetweenChunkIterator::getItem()");

        return _value;
    }

    bool EmptyBitmapBCBetweenChunkIterator::isEmpty() const
    {
        LOG4CXX_DEBUG(logger, "EmptyBitmapBCBetweenChunkIterator::isEmpty()");

        return false;
    }

    EmptyBitmapBCBetweenChunkIterator::EmptyBitmapBCBetweenChunkIterator(BCBetweenChunk const &chunk, int iterationMode)
            : NewBitmapBCBetweenChunkIterator(chunk, iterationMode)
    {
        LOG4CXX_DEBUG(logger, "EmptyBitmapBCBetweenChunkIterator::EmptyBitmapBCBetweenChunkIterator()");

        _value.setBool(true);

        LOG4CXX_DEBUG(logger, "EmptyBitmapBCBetweenChunkIterator::EmptyBitmapBCBetweenChunkIterator() -");
    }

    // Between array iterator methods
    BCBetweenArrayIterator::BCBetweenArrayIterator(BCBetweenArray const &arr, AttributeID attrID, AttributeID inputAttrID)
    : DelegateArrayIterator(arr, attrID, arr.getInputArray()->getConstIterator(inputAttrID)),
      array(arr),
      pos(arr.getArrayDesc().getDimensions().size()),
      _hintForSpatialRanges(0)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator()");
        LOG4CXX_DEBUG(logger, Backtrace().c_str());
                      _spatialRangesChunkPosIteratorPtr = std::shared_ptr<SpatialRangesChunkPosIterator>(
                new SpatialRangesChunkPosIterator(array._spatialRangesPtr, array.getArrayDesc())
        );
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator() - 1");
        reset();

        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator() - 2");
    }

    bool BCBetweenArrayIterator::end()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::end()");

        return !hasCurrent;
    }

    void BCBetweenArrayIterator::operator++()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::operator++()");

        assert(!end());
        assert(!inputIterator->end() && hasCurrent && !_spatialRangesChunkPosIteratorPtr->end());
        assert(_spatialRangesChunkPosIteratorPtr->getPosition() == inputIterator->getPosition());

        advanceToNextChunkInRange();

        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::operator++() - ");
    }

    Coordinates const &BCBetweenArrayIterator::getPosition()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::getPosition()");

        if(!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);

        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::getPosition() -");
        return pos;
    }

    bool BCBetweenArrayIterator::setPosition(Coordinates const &newPos)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::setPosition()");

        Coordinates newChunkPos = newPos;
        array.getArrayDesc().getChunkPositionFor(newChunkPos);

        if (hasCurrent && pos == newChunkPos) {
            return true;
        }

        // If cannot set position in the inputIterator, fail.
        if (! inputIterator->setPosition(newChunkPos)) {
            hasCurrent = false;
            return false;
        }

        // If the position does not correspond to a chunk intersecting some query range, fail.
        if (!array._extendedSpatialRangesPtr->findOneThatContains(newChunkPos, _hintForSpatialRanges)) {
            hasCurrent = false;
            return false;
        }

        // Set position there.
        hasCurrent = true;
        chunkInitialized = false;
        pos = newChunkPos;
        if (_spatialRangesChunkPosIteratorPtr->end() || _spatialRangesChunkPosIteratorPtr->getPosition() > pos) {
            _spatialRangesChunkPosIteratorPtr->reset();
        }
        _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
        assert(_spatialRangesChunkPosIteratorPtr->getPosition() == pos);

        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::setPosition() -");

        return true;
    }

    void BCBetweenArrayIterator::reset()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::reset()");

        chunkInitialized = false;
        inputIterator->reset();
        _spatialRangesChunkPosIteratorPtr->reset();

        // If any of the two iterators is invalid, fail.
        if (inputIterator->end() || _spatialRangesChunkPosIteratorPtr->end())
        {
            hasCurrent = false;
            return;
        }

        // Is inputIterator pointing to a position intersecting some query range?
        pos = inputIterator->getPosition();
        hasCurrent = array._extendedSpatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges);
        if (hasCurrent) {
            assert(pos >= _spatialRangesChunkPosIteratorPtr->getPosition());
            if (pos > _spatialRangesChunkPosIteratorPtr->getPosition()) {
                _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
                assert(!_spatialRangesChunkPosIteratorPtr->end() && pos == _spatialRangesChunkPosIteratorPtr->getPosition());
            }
            return;
        }

        // Is spatialRangesChunkPosIterator pointing to a position that has data?
        Coordinates const& myPos = _spatialRangesChunkPosIteratorPtr->getPosition();
        if (inputIterator->setPosition(myPos)) {
            // The position suggested by _spatialRangesChunkPosIterator exists in inputIterator.
            // Declare victory!
            pos = myPos;
            hasCurrent = true;
            return;
        }
        else {
            // The setPosition, even though unsuccessful, may brought inputInterator to a bad state.
            // Restore to its previous valid state (even though not in any query range).
            bool restored = inputIterator->setPosition(pos);
            SCIDB_ASSERT(restored);
        }

        advanceToNextChunkInRange();

        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::reset() -");
    }

    void BCBetweenArrayIterator::advanceToNextChunkInRange()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::advanceToNextChunkInRange()");

        assert(!inputIterator->end() && !_spatialRangesChunkPosIteratorPtr->end());

        hasCurrent = false;
        chunkInitialized = false;

        while (!inputIterator->end())
        {
            // Increment inputIterator.
            ++(*inputIterator);
            if (inputIterator->end())
            {
                // INPUT ITERATOR END
                assert(hasCurrent == false);

                LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::advanceToNextChunkInRange() inputIterator->end()");
                return;
            }

            pos = inputIterator->getPosition();
            if (array._extendedSpatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges))
            {

                hasCurrent = true;
                _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
                assert(_spatialRangesChunkPosIteratorPtr->getPosition() == pos);

                LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::advanceToNextChunkInRange() array._extendedSpatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)");
                return;
            }

            // Incrementing inputIterator led to a position outside the spatial ranges.
            // We could keep incrementing inputIterator till we find a chunkPos inside a query range, but that
            // can be too slow.
            // So let's try to increment spatialRangesChunkPosIterator also, in every iteration.
            // Whenever one of them (inputIterator or spatialRangesChunkPosIterator) gets there first
            // (i.e. finds a position the other one "like"), the algorithm declares victory.
            //
            // Another note:
            // If advancePositionToAtLeast(pos) advances to a position > pos, we cannot increment spatialRangesChunkPosIterator.
            // The reason is that this new position has not been checked against inputIterator for validity yet, and it will
            // be a mistake to blindly skip it.
            //
            bool advanced = _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
            if (_spatialRangesChunkPosIteratorPtr->end()) {
                assert(hasCurrent == false);

                LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::advanceToNextChunkInRange() _spatialRangesChunkPosIteratorPtr->end()");
                return;
            }
            if (! (advanced && _spatialRangesChunkPosIteratorPtr->getPosition() > pos)) {
                ++(*_spatialRangesChunkPosIteratorPtr);
                if (_spatialRangesChunkPosIteratorPtr->end()) {
                    assert(hasCurrent == false);

                    LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::advanceToNextChunkInRange() !(advanced && _spatialRangesChunkPosIteratorPtr->getPosition() > pos)");
                    return;
                }
            }
            Coordinates const& myPos = _spatialRangesChunkPosIteratorPtr->getPosition();
            if (inputIterator->setPosition(myPos)) {
                // The position suggested by _spatialRangesChunkPosIterator exists in inputIterator.
                // Declare victory!
                pos = myPos;
                hasCurrent = true;

                LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::advanceToNextChunkInRange() inputIterator->setPosition(myPos)");
                return;
            }
            else {
                // The setPosition, even though unsuccessful, may brought inputInterator to a bad state.
                // Restore to its previous valid state (even though not in any query range).
                bool restored = inputIterator->setPosition(pos);
                SCIDB_ASSERT(restored);
            }
        }

        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::advanceToNextChunkInRange() -");
    }

    BCBetweenArray::BCBetweenArray(ArrayDesc const &array, SpatialRangesPtr const &spatialRangesPtr,
                                   std::shared_ptr<Array> const &input)
    : DelegateArray(array, input),
      _spatialRangesPtr(spatialRangesPtr)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArray::BCBetweenArray()");

        // Copy _spatialRangesPtr to extendedSpatialRangesPtr, but reducing low by (interval-1) to cover chunkPos.
        _extendedSpatialRangesPtr = std::make_shared<SpatialRanges>(_spatialRangesPtr->_numDims);
        _extendedSpatialRangesPtr->_ranges.reserve(_spatialRangesPtr->_ranges.size());
        for (size_t i=0; i<_spatialRangesPtr->_ranges.size(); ++i) {
            Coordinates newLow = _spatialRangesPtr->_ranges[i]._low;
            array.getChunkPositionFor(newLow);
            _extendedSpatialRangesPtr->_ranges.push_back(SpatialRange(newLow, _spatialRangesPtr->_ranges[i]._high));
        }

        LOG4CXX_DEBUG(logger, "BCBetweenArray::BCBetweenArray() -");
    }

    DelegateChunk *
    BCBetweenArray::createChunk(DelegateArrayIterator const *iterator, AttributeID attrID) const
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArray::createChunk()");

        return new BCBetweenChunk(*this, *iterator, attrID);
    }

    DelegateArrayIterator *BCBetweenArray::createArrayIterator(AttributeID attrID) const
    {
        LOG4CXX_DEBUG(logger, "BCBetweenArray::createArrayIterator()");

        AttributeID inputAttrID = attrID;
        if (inputAttrID >= inputArray->getArrayDesc().getAttributes().size()) {
            inputAttrID = 0;
        }

        LOG4CXX_DEBUG(logger, "BCBetweenArray::createArrayIterator() -");
        return new BCBetweenArrayIterator(*this, attrID, inputAttrID);
    }
}
