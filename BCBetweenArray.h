/**
 * BEGIN_COPYRIGHT
 *
 * Copyright (C) 2017 RonyK
 * All Rights Reserved.
 *
 * You should have received a copy of the AFFERO GNU General Pulbic License
 * along with SciDB. if not, see <http://www.gnu.org/licenses/agpl-3.0.html>
 *
 * END_COPYRIGHT
 */

//
// Created by rony on 17. 3. 21.
//

#ifndef ML_BETWEEN_BCBETWEENARRAY_H
#define ML_BETWEEN_BCBETWEENARRAY_H

#include <string>
#include <vector>

#include <array/DelegateArray.h>
#include <array/Metadata.h>
#include <query/LogicalExpression.h>
#include <query/Expression.h>
#include <array/SpatialRangesChunkPosIterator.h>

namespace scidb
{
    class BCBetweenArray;
    class BCBetweenArrayIterator;
    class BCBetweenChunkIterator;

    typedef std::shared_ptr<SpatialRanges> SpatialRangesPtr;
    typedef std::shared_ptr<SpatialRangesChunkPosIterator> SpatialRangesChunkPosIteratorPtr;

    class BCBetweenChunk : public DelegateChunk
    {
        friend class BCBetweenChunkIterator;
    public:
        std::shared_ptr<ConstChunkIterator> getConstIterator(int iterationMode) const;
        void setInputChunk(ConstChunk const& inputChunk);
        BCBetweenChunk(BCBetweenArray const &array, DelegateArrayIterator const &iterator, AttributeID attrID);

    private:
        BCBetweenArray const &array;
        SpatialRange myRange;       // the firstPosition and lastPosition of this chunk.
        bool fullyInside;
        bool fullyOutside;
        std::shared_ptr<ConstArrayIterator> emptyBitmapIterator;
    };

    class BCBetweenChunkIterator: public ConstChunkIterator, CoordinatesMapper
    {
    public:
        int getMode() const
        {
            return _mode;
        }

        Value const &getItem();
        bool isEmpty() const;
        bool end();
        void operator ++();
        Coordinates const &getPosition();
        bool setPosition(Coordinates const &pos);
        void reset();
        ConstChunk const &getChunk();

        BCBetweenChunkIterator(BCBetweenChunk const &chunk, int iterationMode);

    protected:
        BCBetweenArray const &array;
        BCBetweenChunk const &chunk;
        std::shared_ptr<ConstChunkIterator> inputIterator;
        Coordinates currPos;
        int _mode;
        bool hasCurrent;
        bool _ignoreEmptyCells;
        MemChunk shapeChunk;
        std::shared_ptr<ConstChunkIterator> emptyBitmapIterator;
        TypeId type;
        Value& evaluate();
        Value& buildBitmpa();
        bool filter();
        void moveNext();
        void nextVisible();

        // Several member functions of class SpatialRanges takes a hint, on where the last successful search
        mutable size_t _hintForSpatialRanges;
    };

    class ExistedBitmapBCBetweenChunkIterator : public BCBetweenChunkIterator
    {
    public:
        virtual Value const &getItem();
        ExistedBitmapBCBetweenChunkIterator(BCBetweenChunk const &chunk, int iterationMode);

    private:
        Value _value;
    };

    class NewBitmapBCBetweenChunkIterator : public BCBetweenChunkIterator
    {
    public:
        virtual Value const &getItem();
        NewBitmapBCBetweenChunkIterator(BCBetweenChunk const &chunk, int iterationMode);

    protected:
        Value _value;
    };

    class EmptyBitmapBCBetweenChunkIterator : public NewBitmapBCBetweenChunkIterator
    {
    public:
        virtual Value const &getItem();
        virtual bool isEmpty() const;

        EmptyBitmapBCBetweenChunkIterator(BCBetweenChunk const &chunk, int iterationMode);
    };

    class BCBetweenArrayIterator : public DelegateArrayIterator
    {
        friend class BCBetweenChunkIterator;
    public:

        /***
         * Constructor for the between iterator
         * Here we initialize the current position vector to all zeros, and obtain an iterator for the appropriate
         * attribute in the input array.
         */
        BCBetweenArrayIterator(BCBetweenArray const& arr, AttributeID attrID, AttributeID inputAttrID);

        /***
         * The end call checks whether we're operating with the last chunk of the between
         * window.
         */
        virtual bool end();

        /***
         * The ++ operator advances the current position to the next chunk of the between
         * window.
         */
        virtual void operator ++();

        /***
         * Simply returns the current position
         * Initial position is a vector of zeros of appropriate dimensionality
         */
        virtual Coordinates const& getPosition();

        /***
         * Here we only need to check that we're not moving beyond the bounds of the between window
         */
        virtual bool setPosition(Coordinates const& pos);

        /***
         * Reset simply changes the current position to all zeros
         */
        virtual void reset();

    protected:
        BCBetweenArray const& array;
        SpatialRangesChunkPosIteratorPtr _spatialRangesChunkPosIteratorPtr;
        Coordinates pos;
        bool hasCurrent;

        /**
         * @see BetweenChunkIterator::_hintForSpatialRanges
         */
        size_t _hintForSpatialRanges;

        /**
         * Increment inputIterator at least once,
         * then advance the two iterators to the next chunk that (a) exists in the database; and (b) intersects a query range.
         *   - Upon success: hasCurrent = true; pos = both iterators' position; chunkInitialized = false;
         *   - Upon failure: hasCurrent = false.
         *
         * @preconditions:
         *   - inputIterator is pointing to a chunk that exists in the database.
         *     (It may or may NOT intersect any query range.)
         *   - spatialRangesChunkPosIteratorPtr is pointing to a chunk intersecting some query range.
         *     (It may or may NOT exist in the database.)
         *
         * @note: by "exists in the database", we mean in the local SciDB instance.
         * @note: in reset(), do NOT call this function if the initial position is already valid.
         */
        void advanceToNextChunkInRange();
    };

    class BCBetweenArrayEptyBitmapIterator : public BCBetweenArrayIterator
    {
        BCBetweenArray &array;
    public:
        virtual ConstChunk const &getChunk();
        BCBetweenArrayEptyBitmapIterator(BCBetweenArray const &array, AttributeID attrID, AttributeID inputAttrID);
    };

    class BCBetweenArray : public DelegateArray
    {
        friend class BCBetweenChunk;
        friend class BCBetweenChunkIterator;
        friend class BCBetweenArrayIterator;
        friend class ExistedBitmapBCBetweenChunkIterator;
        friend class NewBitmapBCBetweenChunkIterator;

    public:
        BCBetweenArray(ArrayDesc const& desc, SpatialRangesPtr const& spatialRangesPtr, std::shared_ptr<Array> const& input);

        virtual DelegateChunk* createChunk(DelegateArrayIterator const* iterator, AttributeID attrID) const;
        virtual DelegateArrayIterator* createArrayIterator(AttributeID attrID) const;


    private:
        /**
         * The original spatial ranges.
         */
        SpatialRangesPtr _spatialRangesPtr;

        /**
         * The modified spatial ranges where every SpatialRange._low is reduced by (interval-1).
         * The goal is to quickly tell, from a chunk's chunkPos, whether the chunk overlaps a spatial range.
         * In particular, a chunk overlaps, if and only if the extended spatial range contains the chunkPos.
         * E.g. Let there be chunk with chunkPos=0 and interval 10. A range [8, 19] intersects the chunk's space,
         * equivalently, the modified range [-1, 19] contains 0.
         */
        SpatialRangesPtr _extendedSpatialRangesPtr;

        std::map<Coordinates, std::shared_ptr<DelegateChunk>, CoordinatesLess > cache;
        Mutex mutex;
        std::shared_ptr<Expression> expression;
        std::vector<BindInfo> bindings;
        bool _tileMode;
        size_t cacheSize;
        AttributeID emptyAttrID;
    };
}
#endif //ML_BETWEEN_BCBETWEENARRAY_H
