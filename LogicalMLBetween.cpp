/*
 * LogicalMLBetween.cpp
 *
 * Created on:Nov 21, 2017
 * Author : RonyK
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb
{
    /**
     * @brief The operator: ml_between().
     *
     * @par Synopsis:
     *      ml_between( srcMultilevelArray, levelFrom, levelTo, {, realLowCoord}+ {, realHighCoord}+ {firstLevelCellWidth}, {, scaleFactor})
     *
     * @par Summary:
     *      Between operator for multi-level array.
     *
     * @par Input:
     *      - the srcMultiLevelArray : a name of multi-level array
     *      - the levelFrom : lowest array level number
     *      - the levelTo : highest array level number
     *      - the real low coordinates : low coordinates of real coordinate
     *
     */

}