/*
 * LogicalBCBetween.cpp
 *
 * Created on :Mar 16, 2017
 * Author : RonyK
 */

#include "query/Operator.h"
#include "system/Exceptions.h"


namespace scidb {

    /**
     * @brief The operator: bc_between().
     *
     * @par Synopsis:
     *   bc_between( srcArray, boundary_expression, {, arrayLowCoord}+ {, arrayHighCoord}+ )
     *
     * @par Summary:
     *   Boundary check between operator.
     *
     * @par Input:
     *   - srcArray : a source array with srcAttrs, and srcDims.
     *   - the boundary_expression : expression for boundary check.
     *   - the array low coordinates : low coordinates of srcArray on each dimension.
     *   - the array high coordinates : high coordinates of srcArray on each dimension.
     *
     * @par Output array:
     *      <
     *          srcAttrs
     *      >
     *      [
     *          srcDims
     *      ]
     *
     */

    class LogicalBCBetween: public  LogicalOperator
    {
    public:
        LogicalBCBetween(const std::string& logicalName, const std::string& alias) : LogicalOperator(logicalName, alias)
        {
            ADD_PARAM_INPUT()
            ADD_PARAM_EXPRESSION(TID_BOOL)
            ADD_PARAM_VARIES()
        }

        std::vector<std::shared_ptr<OperatorParamPlaceholder> > nextVaryParamPlaceholder(const std::vector<ArrayDesc> &schemas)
        {
            std::vector<std::shared_ptr<OperatorParamPlaceholder> > res;
            size_t i = _parameters.size();
            Dimensions const& dims = schemas[0].getDimensions();
            size_t nDims = dims.size();
            if (i < nDims*2 + 1)
                res.push_back(PARAM_CONSTANT(TID_INT64));
            else
                res.push_back(END_OF_VARIES_PARAMS());

            return res;
        }

        ArrayDesc inferSchema(std::vector< ArrayDesc> schemas, std::shared_ptr< Query> query)
        {
            assert(schemas.size() == 1);

            Dimensions const& dims = schemas[0].getDimensions();
            size_t nDims = dims.size();
            assert(_parameters.size() == nDims * 2 + 1);
            assert(_parameters[0]->getParamType() == PARAM_LOGICAL_EXPRESSION);

            return addEmptyTagAttribute(schemas[0]);
        }
    };

    REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalBCBetween, "bc_between");


}  // namespace scidb
