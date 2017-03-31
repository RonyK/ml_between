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

    static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.query.bcbetween"));

    std::string coordinateToString(Coordinates const& coor)
    {
        std::ostringstream oss;
        oss << "[";

        for(int i = 0; i < coor.size(); i++)
        {
            oss << coor[i];
            if(i != coor.size() - 1)
            {
                oss << ",";
            }
        }

        oss << "]";

        return oss.str();
    }
    //
    // Between _chunk methods
    //
    std::shared_ptr<ConstChunkIterator> BCBetweenChunk::getConstIterator(int iterationMode) const
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunk::getConstIterator() + ");
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
                ? (attrID >= _array.getInputArray()->getArrayDesc().getAttributes().size())
                  ? _fullyInside
                    ? (ConstChunkIterator*)new EmptyBitmapBCBetweenChunkIterator(arrayIterator, *this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                    : (ConstChunkIterator*)new NewBitmapBCBetweenChunkIterator(arrayIterator, *this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                  : _fullyInside
                    ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                    : (ConstChunkIterator*)new ExistedBitmapBCBetweenChunkIterator(arrayIterator, *this, iterationMode & ~ConstChunkIterator::IGNORE_DEFAULT_VALUES)
                : _fullyInside
                  ? (ConstChunkIterator*)new DelegateChunkIterator(this, iterationMode)
                  : (ConstChunkIterator*)new BCBetweenChunkIterator(arrayIterator, *this, iterationMode));
    }

    BCBetweenChunk::BCBetweenChunk(BCBetweenArray const& arr, DelegateArrayIterator const& iterator, AttributeID attrID)
            : DelegateChunk(arr, iterator, attrID, false),
              _array(arr),
              _myRange(arr.getArrayDesc().getDimensions().size()),
              _fullyInside(false),
              _fullyOutside(false)
    {
        tileMode = false;
    }

    void BCBetweenChunk::setInputChunk(ConstChunk const& inputChunk)
    {
        DelegateChunk::setInputChunk(inputChunk);
        _myRange._low = inputChunk.getFirstPosition(true);
        _myRange._high = inputChunk.getLastPosition(true);

        // TO-DO: the _fullyInside computation is simple but not optimal.
        // It is possible that the current _chunk is fully inside the union of the specified ranges,
        // although not fully contained in any of them.
        size_t dummy = 0;
        _fullyInside = _array._innerSpatialRnagesPtr->findOneThatContains(_myRange, dummy);
        _fullyOutside = !_array._spatialRangesPtr->findOneThatIntersects(_myRange, dummy);

        isClone = _fullyInside && attrID < _array.getInputArray()->getArrayDesc().getAttributes().size();
        if (_emptyBitmapIterator)
        {
            if (!_emptyBitmapIterator->setPosition(inputChunk.getFirstPosition(false)))
                throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
    }

    inline Value& BCBetweenChunkIterator::evaluate()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::evaluate() +");
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::evaluate() SIZE : " + std::to_string(_array.bindings.size()));
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::evaluate() POS : " + coordinateToString(_curPos));
        for (size_t i = 0, n = _array.bindings.size(); i < n; i++)
        {
            switch (_array.bindings[i].kind)
            {
                case BindInfo::BI_ATTRIBUTE:
                {
                    if(_iterators[i])
                    {
                        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::evaluate() _iterator exist");
                        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::evaluate() _iterator[" + std::to_string(i) + "] : " + coordinateToString(_iterators[i]->getPosition()));
                    }else
                    {
                        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::evaluate() _iterator don't exist");
                    }
                    _params[i] = _iterators[i]->getItem();
                    break;
                }
                case BindInfo::BI_COORDINATE:
                {
                    _params[i].setInt64(inputIterator->getPosition()[_array.bindings[i].resolvedId]);
                    break;
                }
                default:
                    break;
            }
        }

        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::evaluate() -");
        return const_cast<Value&>(_array.expression->evaluate(_params));
    }

    inline bool BCBetweenChunkIterator::filter()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::filter() :" + coordinateToString(_curPos));
        Value const& result = evaluate();
        return !result.isNull() && result.getBool();
    }

    Value const& BCBetweenChunkIterator::getItem()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::getItem() +");
        LOG4CXX_DEBUG(logger, coordinateToString(_curPos));
        if (!_hasCurrent)
        {
            LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::getItem() no _hasCurrent Exception");
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }

        return inputIterator->getItem();
    }

    bool BCBetweenChunkIterator::isEmpty() const
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::isEmpty() +");

        if (!_hasCurrent)
        {
            LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::isEmpty() no _hasCurrent Exception");
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        }
        return inputIterator->isEmpty() ||
               !_array._spatialRangesPtr->findOneThatContains(_curPos, _hintForSpatialRanges);
    }

    bool BCBetweenChunkIterator::end()
    {
        return !_hasCurrent;
    }

    void BCBetweenChunkIterator::operator ++()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::operator ++() IN : " + coordinateToString(_curPos));
        advancedMoveNext();
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::operator ++() OUT : " + coordinateToString(_curPos));
    }

    void BCBetweenChunkIterator::moveNext()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::moveNext() IN : " + coordinateToString(_curPos));
        ++(*inputIterator);
        if (!inputIterator->end())
        {
            for (size_t i = 0, n = _iterators.size(); i < n; i++)
            {
                if (_iterators[i] && _iterators[i] != inputIterator)
                {
                    ++(*_iterators[i]);
                }
            }

            _curPos = inputIterator->getPosition();
        }
    }

    void BCBetweenChunkIterator::advancedMoveNext()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::advancedMoveNext() IN : " + coordinateToString(_curPos));
        moveNext();

        if(_ignoreEmptyCells)
        {
            nextVisible();
        }
    }

    void BCBetweenChunkIterator::nextVisible()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::nextVisible() IN : " + coordinateToString(_curPos));
        while(!inputIterator->end())
        {
            LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::nextVisible() IT : " + coordinateToString(_curPos));
            if(_array._spatialRangesPtr->findOneThatContains(_curPos, _hintForSpatialRanges) && filter())
            {
                LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::nextVisible() FIND : " + coordinateToString(_curPos));
                _hasCurrent = true;
                return;
            }
            moveNext();
        }
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::nextVisible() END _hasCurrent = false");
        _hasCurrent = false;
    }

    Coordinates const& BCBetweenChunkIterator::getPosition()
    {
        return _ignoreEmptyCells ? _curPos : inputIterator->getPosition();
    }

    bool BCBetweenChunkIterator::setPosition(Coordinates const& targetPos)
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::setPosition(" + coordinateToString(targetPos) + ")");

        if(inputIterator->setPosition(targetPos))
        {
            for (size_t i = 0, n = _iterators.size(); i < n; i++)
            {
                if (_iterators[i] && _iterators[i] != inputIterator)
                {
                    if (!_iterators[i]->setPosition(targetPos))
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }
            _curPos = targetPos;
            _hasCurrent = filter();

            if (_ignoreEmptyCells)
            {
                nextVisible();
            }
            return _hasCurrent;
        } else
        {
            _hasCurrent = false;
            return _hasCurrent;
        }
    }

    void BCBetweenChunkIterator::reset()
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::reset() INIT : " + coordinateToString(_curPos));
        inputIterator->reset();
        if (!inputIterator->end())
        {
            for (size_t i = 0, n = _iterators.size(); i < n; i++)
            {
                if (_iterators[i] && _iterators[i] != inputIterator)
                {
                    _iterators[i]->reset();
                }
            }
        }

        nextVisible();
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::reset() FINISH : " + coordinateToString(_curPos));
    }

    ConstChunk const& BCBetweenChunkIterator::getChunk()
    {
        return _chunk;
    }

    BCBetweenChunkIterator::BCBetweenChunkIterator(BCBetweenArrayIterator const& arrayIterator,
                                                   BCBetweenChunk const& aChunk, int iterationMode)
            : CoordinatesMapper(aChunk), DelegateChunkIterator(&aChunk, iterationMode),
              _array(aChunk._array),
              _chunk(aChunk),
              _iterators(_array.bindings.size()),
              _curPos(_array.getArrayDesc().getDimensions().size()),
              _mode(iterationMode & ~INTENDED_TILE_MODE & ~TILE_MODE),
              _ignoreEmptyCells((iterationMode & IGNORE_EMPTY_CELLS) == IGNORE_EMPTY_CELLS),
              _type(_chunk.getAttributeDesc().getType()),
              _hintForSpatialRanges(0),
              _params(*_array.expression),
              _query(Query::getValidQueryPtr(_array._query))
    {
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::BCBetweenChunkIterator() +");
        inputIterator = aChunk.getInputChunk().getConstIterator(iterationMode & ~INTENDED_TILE_MODE);

        for (size_t i = 0, n = _array.bindings.size(); i < n; i++) {
            switch (_array.bindings[i].kind) {
                case BindInfo::BI_COORDINATE:
                {
                    //  if iterator is tile mode, then do something
                    break;
                }
                case BindInfo::BI_ATTRIBUTE:
                {
                    LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::BCBetweenChunkIterator() BI_ATTRIBUTE");
                    if ((AttributeID)_array.bindings[i].resolvedId == arrayIterator._inputAttrID)
                    {
                        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::BCBetweenChunkIterator() INPUT ATTRIBUTE");
                        _iterators[i] = inputIterator;
                    } else
                    {
                        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::BCBetweenChunkIterator() getConstIterator");
                        _iterators[i] = arrayIterator._iterators[i]->getChunk().getConstIterator(IGNORE_EMPTY_CELLS);
                    }
                    break;
                }
                case BindInfo::BI_VALUE:
                {
                    _params[i] = _array.bindings[i].value;
                    break;
                }
                default:
                    break;
            }
        }

        reset();
        nextVisible();
        LOG4CXX_DEBUG(logger, "BCBetweenChunkIterator::BCBetweenChunkIterator() -");
    }

    //
    // Exited bitmap _chunk iterator methods
    //
    Value const& ExistedBitmapBCBetweenChunkIterator::getItem()
    {
        _value.setBool(
                inputIterator->getItem().getBool() &&
                _array._spatialRangesPtr->findOneThatContains(_curPos, _hintForSpatialRanges) &&
                filter()
        );
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
        _value.setBool(_array._spatialRangesPtr->findOneThatContains(_curPos, _hintForSpatialRanges));
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
        chunk = _array.getEmptyBitmapChunk(this);
        return *chunk->materialize();
    }

    BCBetweenArrayEmptyBitmapIterator::BCBetweenArrayEmptyBitmapIterator(BCBetweenArray const& arr, AttributeID outAttrID, AttributeID inAttrID)
            : BCBetweenArrayIterator(arr, outAttrID, inAttrID),
              _array((BCBetweenArray&)arr)
    {}

    //
    // Between _array iterator methods
    //
    BCBetweenArrayIterator::BCBetweenArrayIterator(BCBetweenArray const& arr, AttributeID attrID, AttributeID inputAttrID)
            : DelegateArrayIterator(arr, attrID, arr.getInputArray()->getConstIterator(inputAttrID)),
              _array(arr),
              _curPos(arr.getArrayDesc().getDimensions().size()),
              _hintForSpatialRanges(0),
              _iterators(arr.bindings.size()),
              _inputAttrID(inputAttrID)
    {
        _spatialRangesChunkPosIteratorPtr = std::shared_ptr<SpatialRangesChunkPosIterator>(
                new SpatialRangesChunkPosIterator(_array._spatialRangesPtr, _array.getArrayDesc()));

        for (size_t i = 0, n = _iterators.size(); i < n; i++)
        {
            switch (_array.bindings[i].kind)
            {
                case BindInfo::BI_ATTRIBUTE:
                {
                    LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator() BI_ATTRIBUTE");
                    if ((AttributeID)_array.bindings[i].resolvedId == inputAttrID)
                    {
                        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator() INPUT ATTRIBUTE");
                        _iterators[i] = inputIterator;
                    } else
                    {
                        LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator() getConstIterator");
                        _iterators[i] = _array.getInputArray()->
                                getConstIterator(safe_static_cast<AttributeID>(_array.bindings[i].resolvedId));
                    }
                    break;
                }
                case BindInfo::BI_COORDINATE:
                {
                    // If tilemode, do something.
                    break;
                }
                default:
                    break;
            }
        }

        reset();
    }

    bool BCBetweenArrayIterator::end()
    {
        return !_hasCurrent;
    }

    Coordinates const& BCBetweenArrayIterator::getPosition()
    {
        if (!_hasCurrent)
            throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_NO_CURRENT_ELEMENT);
        return _curPos;
    }

    bool BCBetweenArrayIterator::setPosition(Coordinates const& newPos)
    {
        Coordinates newChunkPos = newPos;
        _array.getArrayDesc().getChunkPositionFor(newChunkPos);

        if (_hasCurrent && _curPos == newChunkPos) {
            return true;
        }

        // If the position does not correspond to a _chunk intersecting some query range, fail.
        if (!_array._extendedSpatialRangesPtr->findOneThatContains(newChunkPos, _hintForSpatialRanges))
        {
            _hasCurrent = false;
            return false;
        }

        // Set position there.
        _hasCurrent = true;
        chunkInitialized = false;
        _curPos = newChunkPos;
        if (_spatialRangesChunkPosIteratorPtr->end() || _spatialRangesChunkPosIteratorPtr->getPosition() > _curPos)
        {
            _spatialRangesChunkPosIteratorPtr->reset();
        }
        _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(_curPos);
        assert(_spatialRangesChunkPosIteratorPtr->getPosition() == _curPos);

        // Set all iterators position.
        return setAllIteratorsPosition(_curPos);
    }

    ConstChunk const& BCBetweenArrayIterator::getChunk()
    {
        chunk->setInputChunk(inputIterator->getChunk());
        chunk->overrideClone(false);
        return *chunk;
    }

    void BCBetweenArrayIterator::moveNext()
    {
        chunkInitialized = false;
        ++(*inputIterator);
        for (size_t i = 0, n = _iterators.size(); i < n; i++)
        {
            if (_iterators[i] && _iterators[i] != inputIterator)
            {
                ++(*_iterators[i]);
            }
        }
        if (_emptyBitmapIterator)
        {
            ++(*_emptyBitmapIterator);
        }
    }

    void BCBetweenArrayIterator::advanceToNextChunkInRange()
    {
        assert(!inputIterator->end() && !_spatialRangesChunkPosIteratorPtr->end());

        _hasCurrent = false;
        chunkInitialized = false;

        while (!inputIterator->end())
        {
            // Increment _inputIterator.
            moveNext();
            if (inputIterator->end())
            {
                assert(_hasCurrent == false);
                return;
            }
            _curPos = inputIterator->getPosition();
            if (_array._extendedSpatialRangesPtr->findOneThatContains(_curPos, _hintForSpatialRanges))
            {
                _hasCurrent = true;
                _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(_curPos);
                assert(_spatialRangesChunkPosIteratorPtr->getPosition() == _curPos);
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
            // If advancePositionToAtLeast(_curPos) advances to a position > _curPos, we cannot increment spatialRangesChunkPosIterator.
            // The reason is that this new position has not been checked against _inputIterator for validity yet, and it will
            // be a mistake to blindly skip it.
            //
            bool advanced = _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(_curPos);
            if (_spatialRangesChunkPosIteratorPtr->end())
            {
                assert(_hasCurrent == false);
                return;
            }
            if (! (advanced && _spatialRangesChunkPosIteratorPtr->getPosition() > _curPos))
            {
                ++(*_spatialRangesChunkPosIteratorPtr);
                if (_spatialRangesChunkPosIteratorPtr->end())
                {
                    assert(_hasCurrent == false);
                    return;
                }
            }
            Coordinates const& myPos = _spatialRangesChunkPosIteratorPtr->getPosition();
            if (inputIterator->setPosition(myPos))
            {
                LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator() SET NEW POS : " + coordinateToString(myPos));
                setAllIteratorsPosition(myPos);
                // The position suggested by _spatialRangesChunkPosIterator exists in _inputIterator.
                // Declare victory!
                _curPos = myPos;
                _hasCurrent = true;
                return;
            } else
            {
                // The setPosition, even though unsuccessful, may brought inputInterator to a bad state.
                // Restore to its previous valid state (even though not in any query range).
                LOG4CXX_DEBUG(logger, "BCBetweenArrayIterator::BCBetweenArrayIterator() RESTORE POS : " + coordinateToString(_curPos));

                bool restored = inputIterator->setPosition(_curPos);
                setAllIteratorsPosition(_curPos);
                SCIDB_ASSERT(restored);
            }
        }
    }

    void BCBetweenArrayIterator::operator ++()
    {
        assert(!end());
        assert(!inputIterator->end() && _hasCurrent && !_spatialRangesChunkPosIteratorPtr->end());
        assert(_spatialRangesChunkPosIteratorPtr->getPosition() == inputIterator->getPosition());

        advanceToNextChunkInRange();
    }

    void BCBetweenArrayIterator::reset()
    {
        chunkInitialized = false;
        inputIterator->reset();
        _spatialRangesChunkPosIteratorPtr->reset();

        // If any of the two _iterators is invalid, fail.
        if (inputIterator->end() || _spatialRangesChunkPosIteratorPtr->end())
        {
            _hasCurrent = false;
            return;
        }

        // Is _inputIterator pointing to a position intersecting some query range?
        _curPos = inputIterator->getPosition();
        _hasCurrent = _array._extendedSpatialRangesPtr->findOneThatContains(_curPos, _hintForSpatialRanges);
        if (_hasCurrent)
        {
            assert(_curPos >= _spatialRangesChunkPosIteratorPtr->getPosition());
            if (_curPos > _spatialRangesChunkPosIteratorPtr->getPosition())
            {
                _spatialRangesChunkPosIteratorPtr->advancePositionToAtLeast(_curPos);
                assert(!_spatialRangesChunkPosIteratorPtr->end() && _curPos == _spatialRangesChunkPosIteratorPtr->getPosition());
            }

            for (size_t i = 0, n = _iterators.size(); i < n; i++)
            {
                if (_iterators[i] && _iterators[i] != inputIterator)
                {
                    _iterators[i]->reset();
                }
            }
            if (_emptyBitmapIterator)
            {
                _emptyBitmapIterator->reset();
            }
            return;
        }

        // Is spatialRangesChunkPosIterator pointing to a position that has data?
        Coordinates const& myPos = _spatialRangesChunkPosIteratorPtr->getPosition();
        if (inputIterator->setPosition(myPos))
        {
            // The position suggested by _spatialRangesChunkPosIterator exists in _inputIterator.
            // Declare victory!
            setAllIteratorsPosition(myPos);
            _curPos = myPos;
            _hasCurrent = true;

            return;
        } else {
            // The setPosition, even though unsuccessful, may brought inputInterator to a bad state.
            // Restore to its previous valid state (even though not in any query range).
            bool restored = inputIterator->setPosition(_curPos);
            SCIDB_ASSERT(restored);
            setAllIteratorsPosition(_curPos);
        }

        advanceToNextChunkInRange();
    }

    bool BCBetweenArrayIterator::setAllIteratorsPosition(Coordinates const &pos)
    {
        if(inputIterator->setPosition(pos))
        {
            for(size_t i = 0, n = _iterators.size(); i < n; i++)
            {
                if(_iterators[i])
                {
                    if(!_iterators[i]->setPosition(pos))
                        throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
                }
            }

            if(_emptyBitmapIterator)
            {
                if(!_emptyBitmapIterator->setPosition(pos))
                    throw USER_EXCEPTION(SCIDB_SE_EXECUTION, SCIDB_LE_OPERATION_FAILED) << "setPosition";
            }
            return true;
        }

        return false;
    }

    //
    // Between _array methods
    //
    BCBetweenArray::BCBetweenArray(ArrayDesc const& array,
                                   SpatialRangesPtr const& spatialRangesPtr,
                                   SpatialRangesPtr const& innerSpatialRangesPtr,
                                   std::shared_ptr<Array> const& input,
                                   std::shared_ptr<Expression> expr,
                                   std::shared_ptr<Query>& query,
                                   bool tileMode)
            : DelegateArray(array, input),
              _spatialRangesPtr(spatialRangesPtr),
              _innerSpatialRnagesPtr(innerSpatialRangesPtr),
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

        return new BCBetweenArrayIterator(*this, attrID, inputAttrID);
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
            if (chunk)
            {
                return chunk;
            }
        }
        chunk = std::shared_ptr<DelegateChunk>(createChunk(iterator, emptyAttrID));
        chunk->setInputChunk(iterator->getInputIterator()->getChunk());
        chunk->materialize();
        {
            ScopedMutexLock cs(mutex);
            if (cache.size() >= cacheSize)
            {
                cache.erase(cache.begin());
            }
            cache[pos] = chunk;
        }
        return chunk;
    }
}
