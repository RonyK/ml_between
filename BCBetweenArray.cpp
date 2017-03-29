/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2015 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/**
 * @file BetweenArray.cpp
 *
 * @brief Between array implementation
 *
 * @author Konstantin Knizhnik <knizhnik@garret.ru>
 */

#include "BCBetweenArray.h"
#include <system/Exceptions.h>
#include <util/SpatialType.h>
#include <system/Utils.h>

namespace scidb
{
    using namespace boost;

    //
    // Between _chunk methods
    //
    std::shared_ptr<ConstChunkIterator> BCBetweenChunk::getConstIterator(int iterationMode) const
    {

//        return std::shared_ptr<ConstChunkIterator>(
//                attr.isEmptyIndicator()
//                ? (attr.getId() >= inputArray->getArrayDesc().getAttributes().size())
//                  ? (DelegateChunkIterator*)new NewBitmapBCBetweenChunkIterator(arrayIterator, chunk, iterationMode)
//                  : (DelegateChunkIterator*)new ExistedBitmapBCBetweenChunkIterator(arrayIterator, chunk, iterationMode)
//                : (DelegateChunkIterator*)new BCBetweenChunkIterator(arrayIterator, chunk, iterationMode)
//        );

        BCBetweenArrayIterator arrayIterator = (BCBetweenArrayIterator const&)getArrayIterator();
        AttributeDesc const& attr = getAttributeDesc();
        // TileMode is false.
        // Defined in BCBetweenChunk::BCBetweenChunk
        if (tileMode/* && chunk->isRLE()*/)
        {
            iterationMode |= ChunkIterator::TILE_MODE;
        } else
        {
            iterationMode &= ~ChunkIterator::TILE_MODE;
        }
        iterationMode &= ~ChunkIterator::INTENDED_TILE_MODE;
        return std::shared_ptr<ConstChunkIterator>(
                attr.isEmptyIndicator()
                ? (attrID >= array.getInputArray()->getArrayDesc().getAttributes().size())
                  ? fullyInside
                    ? (ConstChunkIterator*)new EmptyBitmapBCBetweenChunkIterator(arrayIterator, *this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                    : (ConstChunkIterator*)new NewBitmapBCBetweenChunkIterator(arrayIterator, *this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                  : fullyInside
                    ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                    : (ConstChunkIterator*)new ExistedBitmapBCBetweenChunkIterator(arrayIterator, *this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                : fullyInside
                  ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode)
                  : (ConstChunkIterator*)new BCBetweenChunkIterator(arrayIterator, *this, iterationMode));
    }

    BCBetweenChunk::BCBetweenChunk(BCBetweenArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
            : DelegateChunk(arr, iterator, attrID, false),
              array(arr),
              myRange(arr.getArrayDesc().getDimensions().size()),
              fullyInside(false),
              fullyOutside(false)
    {
        tileMode = false;
    }

    void BCBetweenChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        myRange._low = inputChunk.getFirstPosition(true);
        myRange._high = inputChunk.getLastPosition(true);

        // TO-DO: the fullyInside computation is simple but not optimal.
        // It is possible that the current _chunk is fully inside the union of the specified ranges,
        // although not fully contained in any of them.
        size_t dummy = 0;
        fullyInside  =  array._spatialRangesPtr->findOneThatContains(myRange, dummy);
        fullyOutside = !array._spatialRangesPtr->findOneThatIntersects(myRange, dummy);

        isClone = fullyInside && attrID < array.getInputArray()->getArrayDesc().getAttributes().size();
        if (emptyBitmapIterator) {
            if (!emptyBitmapIterator->setPosition(inputChunk.getFirstPosition(false)))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }

    inline Value& BCBetweenChunkIterator::evaluate()
    {
        for (size_t i = 0, n = _array.bindings.size(); i < n; i++)
        {
            switch (_array.bindings[i].kind)
            {
                case BindInfo::BI_ATTRIBUTE:
                    _params[i] = _iterators[i]->getItem();
                    break;
                case BindInfo::BI_COORDINATE:
                    if (_mode & TILE_MODE)
                    {
                        _iterators[i]->getItem().getTile()->getCoordinates(
                                _array.getInputArray()->getArrayDesc(),
                                _array.bindings[i].resolvedId,
                                _iterators[i]->getChunk().getFirstPosition(false),
                                _iterators[i]->getPosition(),
                                _query,
                                _params[i],
                                !(_mode & IGNORE_OVERLAPS));
                    } else
                    {
                        _params[i].setInt64(_inputIterator->getPosition()[_array.bindings[i].resolvedId]);
                    }
                    break;

                default:
                    break;
            }
        }

        return const_cast<Value&>(_array.expression->evaluate(_params));
    }

    inline bool BCBetweenChunkIterator::filter()
    {
        Value const& result = evaluate();
        return !result.isNull() && result.getBool();
    }

    void BCBetweenChunkIterator::moveNext()
    {
        ++(*inputIterator);
        if (!inputIterator->end()) {
            for (size_t i = 0, n = _iterators.size(); i < n; i++) {
                if (_iterators[i] && _iterators[i] != inputIterator) {
                    ++(*_iterators[i]);
                }
            }
        }
    }

    void BCBetweenChunkIterator::nextVisible()
    {
        while (!inputIterator->end()) {
            if ((_mode & TILE_MODE) || filter()) {
                _hasCurrent = true;
                return;
            }
            moveNext();
        }
        _hasCurrent = false;
    }

    inline Value& BCBetweenChunkIterator::buildBitmap()
    {
        Value& value = evaluate();
        RLEPayload* inputPayload = value.getTile();
        RLEPayload::append_iterator appender(_tileValue.getTile());
        RLEPayload::iterator vi(inputPayload);
        if (!_emptyBitmapIterator->setPosition(inputIterator->getPosition()))
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        RLEPayload* emptyBitmap = _emptyBitmapIterator->getItem().getTile();
        RLEPayload::iterator ei(emptyBitmap);

#ifndef NDEBUG
        position_t prevPos = 0;
#endif

        Value trueVal, falseVal;
        trueVal.setBool(true);
        falseVal.setBool(false);
        while (!ei.end()) {
#ifndef NDEBUG
            position_t currPos = ei.getPPos();
#endif
            assert (prevPos == currPos);
            uint64_t count;
            if (ei.checkBit()) {
                count = min(vi.getRepeatCount(), ei.getRepeatCount());
                appender.add((vi.isNull()==false && vi.checkBit()) ? trueVal : falseVal, count);
                vi += count;
            } else {
                count = ei.getRepeatCount();
                appender.add(falseVal, count);
            }
            ei += count;

#ifndef NDEBUG
            prevPos = currPos + count;
#endif
        }
        appender.flush();
        return _tileValue;
    }

    Value const& BCBetweenChunkIterator::getItem()
    {
        if (!_hasCurrent)
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        if (_mode & TILE_MODE) {
            RLEPayload* newEmptyBitmap = evaluate().getTile();
            RLEPayload::iterator ei(newEmptyBitmap);
            Value const& value = inputIterator->getItem();
            RLEPayload* inputPayload = value.getTile();
            RLEPayload::iterator vi(inputPayload);

            // This needs to compare against getMaxLength() or multiple tests
            // will fail with scidb::SCIDB_SE_NETWORK::SCIDB_LE_CANT_SEND_RECEIVE.
            if (newEmptyBitmap->count() == CoordinateBounds::getMaxLength()) {
                assert(newEmptyBitmap->nSegments() == 1);
                if (ei.isNull() == false && ei.checkBit()) {
                    // empty bitmap containing all ones: just return original value
                    return value;
                }
                _tileValue.getTile()->clear();
            } else {
                RLEPayload::append_iterator appender(_tileValue.getTile());
                Value v;
                while (!ei.end()) {
                    uint64_t count = ei.getRepeatCount();
                    if (ei.isNull() == false && ei.checkBit()) {
                        count = appender.add(vi, count);
                    } else {
                        vi += count;
                    }
                    ei += count;
                }
                appender.flush();
            }
            return _tileValue;
        }

        return _inputIterator->getItem();
    }

    bool BCBetweenChunkIterator::isEmpty() const
    {
        if (!_hasCurrent)
        {
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        return _inputIterator->isEmpty() ||
               !_array._spatialRangesPtr->findOneThatContains(_currPos, _hintForSpatialRanges);
    }

    bool BCBetweenChunkIterator::end()
    {
        return !_hasCurrent;
    }

    void BCBetweenChunkIterator::operator ++()
    {
        if (_ignoreEmptyCells)
        {
            while (true)
            {
                ++(*_inputIterator);
                if (!_inputIterator->end())
                {
                    Coordinates const& pos = _inputIterator->getPosition();

                    if (_array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges))
                    {
                        _currPos = pos;
                        _hasCurrent = true;
                        return;
                    }
                } else {
                    break;
                }
            }
            _hasCurrent = false;
        } else
        {
            ++(*_inputIterator);
            _hasCurrent = !_inputIterator->end();
        }
    }

    Coordinates const& BCBetweenChunkIterator::getPosition()
    {
        return _ignoreEmptyCells ? _currPos : _inputIterator->getPosition();
    }

    bool BCBetweenChunkIterator::setPosition(Coordinates const& pos)
    {
        if (_ignoreEmptyCells) {
            if (_array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                _hasCurrent = _inputIterator->setPosition(pos);
                if (_hasCurrent) {
                    _currPos = pos;
                }
                return _hasCurrent;
            }
            _hasCurrent = false;
            return false;
        }

        _hasCurrent = _inputIterator->setPosition(pos);
        return _hasCurrent;
    }

    void BCBetweenChunkIterator::reset()
    {
        if ( _ignoreEmptyCells ) {
            _inputIterator->reset();
            if (!_inputIterator->end()) {
                Coordinates const& pos = _inputIterator->getPosition();
                if (_array._spatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                    _currPos = pos;
                    _hasCurrent = true;
                    return;
                }
                else {
                    ++(*this);  // The operator++() will skip the cells outside all the requested ranges.
                }
            }
            else {
                _hasCurrent = false;
            }
        } else {
            _inputIterator->reset();
            _hasCurrent = !_inputIterator->end();
        }
    }

    ConstChunk const& BCBetweenChunkIterator::getChunk()
    {
        return _chunk;
    }

    BCBetweenChunkIterator::BCBetweenChunkIterator(BCBetweenArrayIterator const& arrayIterator,
                                                   BCBetweenChunk const& aChunk, int iterationMode)
            : CoordinatesMapper(aChunk), DelegateChunkIterator(&aChunk, iterationMode),
              _array(aChunk.array),
              _chunk(aChunk),
              _inputIterator(aChunk.getInputChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE)),
              _iterators(_array.bindings.size()),
              _currPos(_array.getArrayDesc().getDimensions().size()),
              _mode(iterationMode & ~INTENDED_TILE_MODE & ~TILE_MODE),
              _ignoreEmptyCells((iterationMode & IGNORE_EMPTY_CELLS) == IGNORE_EMPTY_CELLS),
              _type(_chunk.getAttributeDesc().getType()),
              _hintForSpatialRanges(0),
              _params(*_array.expression),
              _query(Query::getValidQueryPtr(_array._query))
    {
        reset();

        for (size_t i = 0, n = _array.bindings.size(); i < n; i++) {
            switch (_array.bindings[i].kind) {
                case BindInfo::BI_COORDINATE:
                    if (_mode & TILE_MODE) {
                        if (arrayIterator.iterators[i] == arrayIterator.getInputIterator()) {
                            _iterators[i] = inputIterator;
                        } else {
                            _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator(iterationMode);

                        }
                    }
                    break;
                case BindInfo::BI_ATTRIBUTE:
                    if ((AttributeID)_array.bindings[i].resolvedId == arrayIterator.inputAttrID) {
                        _iterators[i] = inputIterator;
                    } else {
                        _iterators[i] = arrayIterator.iterators[i]->getChunk().getConstIterator((_mode & TILE_MODE)|IGNORE_EMPTY_CELLS);
                    }
                    break;
                case BindInfo::BI_VALUE:
                    _params[i] = _array.bindings[i].value;
                    break;
                default:
                    break;
            }
        }
        if (iterationMode & TILE_MODE) {
            _tileValue = Value(TypeLibrary::getType(chunk->getAttributeDesc().getType()),Value::asTile);
            if (arrayIterator.emptyBitmapIterator) {
                _emptyBitmapIterator = arrayIterator.emptyBitmapIterator->getChunk().getConstIterator(TILE_MODE|IGNORE_EMPTY_CELLS);
            } else {
                ArrayDesc const& arrayDesc = chunk->getArrayDesc();
                Address addr(arrayDesc.getEmptyBitmapAttribute()->getId(), chunk->getFirstPosition(false));
                _shapeChunk.initialize(&_array, &arrayDesc, addr, 0);
                _emptyBitmapIterator = _shapeChunk.getConstIterator(TILE_MODE|IGNORE_EMPTY_CELLS);
            }
        }
        nextVisible();
    }

    //
    // Exited bitmap _chunk iterator methods
    //
    Value const& ExistedBitmapBCBetweenChunkIterator::getItem()
    {
        // TODO :: Optimize here
//        if (_mode & TILE_MODE) {
//            return buildBitmap();
//        } else {
        _value.setBool(
                _inputIterator->getItem().getBool() &&
                _array._spatialRangesPtr->findOneThatContains(_currPos, _hintForSpatialRanges) &&
                filter()
        );
//        }
        return _value;
    }

    ExistedBitmapBCBetweenChunkIterator::ExistedBitmapBCBetweenChunkIterator(BCBetweenArrayIterator const& arrayIterator, BCBetweenChunk const& chunk, int iterationMode)
            : BCBetweenChunkIterator(arrayIterator, chunk, iterationMode),
              _value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // New bitmap _chunk iterator methods
    //
    Value const& NewBitmapBCBetweenChunkIterator::getItem()
    {
        _value.setBool(_array._spatialRangesPtr->findOneThatContains(_currPos, _hintForSpatialRanges));
        if(_value.getBool())
        {
            return evaluate();
        }

        return _value;
    }

    NewBitmapBCBetweenChunkIterator::NewBitmapBCBetweenChunkIterator(BCBetweenArrayIterator const& arrayIterator, BCBetweenChunk const& chunk, int iterationMode)
            : BCBetweenChunkIterator(arrayIterator, chunk, iterationMode),
              _value(TypeLibrary::getType(TID_BOOL))
    {
    }

    //
    // Empty bitmap _chunk iterator methods
    //
    Value const& EmptyBitmapBCBetweenChunkIterator::getItem()
    {
        return _value;
    }

    bool EmptyBitmapBCBetweenChunkIterator::isEmpty() const
    {
        return false;
    }

    EmptyBitmapBCBetweenChunkIterator::EmptyBitmapBCBetweenChunkIterator(BCBetweenArrayIterator const& arrayIterator, BCBetweenChunk const& chunk, int iterationMode)
            : NewBitmapBCBetweenChunkIterator(arrayIterator, chunk, iterationMode)
    {
        _value.setBool(true);
    }

    //
    // BCBetweenArrayEmptyBitmapIterator methods
    //
    ConstChunk const& BCBetweenArrayEmptyBitmapIterator::getChunk()
    {
        chunk = array.getEmptyBitmapChunk(this);
        return *chunk->materialize();
    }

    BCBetweenArrayEmptyBitmapIterator::BCBetweenArrayEmptyBitmapIterator(BCBetweenArray const& arr, AttributeID outAttrID, AttributeID inAttrID)
            : BCBetweenArrayIterator(arr, outAttrID, inAttrID),
              array((BCBetweenArray&)arr)
    {}

    //
    // Between _array iterator methods
    //
    BCBetweenArrayIterator::BCBetweenArrayIterator(BCBetweenArray const& arr, AttributeID attrID, AttributeID inputAttrID)
            : DelegateArrayIterator(arr, attrID, arr.getInputArray()->getConstIterator(inputAttrID)),
              array(arr),
              pos(arr.getArrayDesc().getDimensions().size()),
              _hintForSpatialRanges(0)
    {
        _spatialRangesChunkPosIteratorPtr = std::shared_ptr<SpatialRangesChunkPosIterator>(
                new SpatialRangesChunkPosIterator(array._spatialRangesPtr, array.getArrayDesc()));
        reset();
    }

    bool BCBetweenArrayIterator::end()
    {
        return !hasCurrent;
    }

    Coordinates const& BCBetweenArrayIterator::getPosition()
    {
        if (!hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return pos;
    }

    bool BCBetweenArrayIterator::setPosition(Coordinates const& newPos)
    {
        Coordinates newChunkPos = newPos;
        array.getArrayDesc().getChunkPositionFor(newChunkPos);

        if (hasCurrent && pos == newChunkPos) {
            return true;
        }

        // If cannot set position in the _inputIterator, fail.
        if (! inputIterator->setPosition(newChunkPos)) {
            hasCurrent = false;
            return false;
        }

        // If the position does not correspond to a _chunk intersecting some query range, fail.
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

        return true;
    }

    ConstChunk const& BCBetweenArrayIterator::getChunk()
    {
        chunk->setInputChunk(inputIterator->getChunk());
        chunk->overrideClone(false);
        return *chunk;
    }

    void BCBetweenArrayIterator::advanceToNextChunkInRange()
    {
        assert(!inputIterator->end() && !_spatialRangesChunkPosIteratorPtr->end());

        hasCurrent = false;
        chunkInitialized = false;

        while (!inputIterator->end())
        {
            // Increment _inputIterator.
            ++(*inputIterator);
            if (inputIterator->end()) {
                assert(hasCurrent == false);
                return;
            }
            pos = inputIterator->getPosition();
            if (array._extendedSpatialRangesPtr->findOneThatContains(pos, _hintForSpatialRanges)) {
                hasCurrent = true;
                _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
                assert(_spatialRangesChunkPosIteratorPtr->getPosition() == pos);
                return;
            }

            // Incrementing _inputIterator led to a position outside the spatial ranges.
            // We could keep incrementing _inputIterator till we find a chunkPos inside a query range, but that
            // can be too slow.
            // So let's try to increment spatialRangesChunkPosIterator also, in every iteration.
            // Whenever one of them (_inputIterator or spatialRangesChunkPosIterator) gets there first
            // (i.e. finds a position the other one "like"), the algorithm declares victory.
            //
            // Another note:
            // If advancePositionToAtLeast(pos) advances to a position > pos, we cannot increment spatialRangesChunkPosIterator.
            // The reason is that this new position has not been checked against _inputIterator for validity yet, and it will
            // be a mistake to blindly skip it.
            //
            bool advanced = _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(pos);
            if (_spatialRangesChunkPosIteratorPtr->end()) {
                assert(hasCurrent == false);
                return;
            }
            if (! (advanced && _spatialRangesChunkPosIteratorPtr->getPosition() > pos)) {
                ++(*_spatialRangesChunkPosIteratorPtr);
                if (_spatialRangesChunkPosIteratorPtr->end()) {
                    assert(hasCurrent == false);
                    return;
                }
            }
            Coordinates const& myPos = _spatialRangesChunkPosIteratorPtr->getPosition();
            if (inputIterator->setPosition(myPos)) {
                // The position suggested by _spatialRangesChunkPosIterator exists in _inputIterator.
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
        }
    }

    void BCBetweenArrayIterator::operator ++()
    {
        assert(!end());
        assert(!inputIterator->end() && hasCurrent && !_spatialRangesChunkPosIteratorPtr->end());
        assert(_spatialRangesChunkPosIteratorPtr->getPosition() == inputIterator->getPosition());

        advanceToNextChunkInRange();
    }

    void BCBetweenArrayIterator::reset()
    {
        chunkInitialized = false;
        inputIterator->reset();
        _spatialRangesChunkPosIteratorPtr->reset();

        // If any of the two iterators is invalid, fail.
        if (inputIterator->end() || _spatialRangesChunkPosIteratorPtr->end())
        {
            hasCurrent = false;
            return;
        }

        // Is _inputIterator pointing to a position intersecting some query range?
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
            // The position suggested by _spatialRangesChunkPosIterator exists in _inputIterator.
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
    }

    //
    // Between _array methods
    //
    BCBetweenArray::BCBetweenArray(ArrayDesc const& array,
                                   SpatialRangesPtr const& spatialRangesPtr,
                                   std::shared_ptr<Array> const& input,
                                   std::shared_ptr<Expression> expr,
                                   std::shared_ptr<Query>& query,
                                   bool tileMode)
            : DelegateArray(array, input),
              _spatialRangesPtr(spatialRangesPtr),
              expression(expr),
              bindings(expr->getBindings()),
              _tileMode(tileMode),
              cacheSize(Config::getInstance()->getOption<int>(CONFIG_RESULT_PREFETCH_QUEUE_SIZE)),
              emptyAttrID(desc.getEmptyBitmapAttribute()->getId())
    {
        assert(query);
        _query = query;

        // Copy _spatialRangesPtr to extendedSpatialRangesPtr, but reducing low by (interval-1) to cover chunkPos.
        _extendedSpatialRangesPtr = make_shared<SpatialRanges>(_spatialRangesPtr->_numDims);
        _extendedSpatialRangesPtr->_ranges.reserve(_spatialRangesPtr->_ranges.size());
        for (size_t i=0; i<_spatialRangesPtr->_ranges.size(); ++i) {
            Coordinates newLow = _spatialRangesPtr->_ranges[i]._low;
            array.getChunkPositionFor(newLow);
            _extendedSpatialRangesPtr->_ranges.push_back(SpatialRange(newLow, _spatialRangesPtr->_ranges[i]._high));
        }
    }

    DelegateArrayIterator* BCBetweenArray::createArrayIterator(AttributeID attrID) const
    {
        AttributeID inputAttrID = attrID;
        if (inputAttrID >= inputArray->getArrayDesc().getAttributes().size())
        {
            inputAttrID = 0;
            for (size_t i = 0, n = bindings.size(); i < n; i++)
            {
                if (bindings[i].kind == BindInfo::BI_ATTRIBUTE)
                {
                    inputAttrID = (AttributeID)bindings[i].resolvedId;
                    break;
                }
            }
        }

#if FILTER_CHUNK_CACHE
        return attrID == emptyAttrID
            ? (DelegateArrayIterator*)new BCBetweenArrayEmptyBitmapIterator(*this, attrID, inputAttrID)
            : (DelegateArrayIterator*)new BCBetweenArrayIterator(*this, attrID, inputAttrID);
#else
        return new BCBetweenArrayIterator(*this, attrID, inputAttrID);
#endif
    }

    DelegateChunk* BCBetweenArray::createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const
    {
        return new BCBetweenChunk(*this, *iterator, attrID);
    }

    std::shared_ptr<DelegateChunk> BCBetweenArray::getEmptyBitmapChunk(BCBetweenArrayEmptyBitmapIterator* iterator)
    {
        std::shared_ptr<DelegateChunk> chunk;
        Coordinates const& pos = iterator->getPosition();
        {
            ScopedMutexLock cs(mutex);
            chunk = cache[pos];
            if (chunk) {
                return chunk;
            }
        }
        chunk = std::shared_ptr<DelegateChunk>(createChunk(iterator, emptyAttrID));
        chunk->setInputChunk(iterator->getInputIterator()->getChunk());
        chunk->materialize();
        {
            ScopedMutexLock cs(mutex);
            if (cache.size() >= cacheSize) {
                cache.erase(cache.begin());
            }
            cache[pos] = chunk;
        }
        return chunk;
    }
}
