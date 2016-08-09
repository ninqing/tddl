package com.taobao.tddl.repo.mysql.function;
//package com.taobao.tddl.repo.mysql.function;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
//import com.taobao.tddl.optimizer.core.expression.IFunction;
//
//public class FunctionStringConstructorManager {
//
//    static protected Map<String, FunctionStringConstructor> constructors = null;
//
//    static {
//        constructors = new HashMap<String, FunctionStringConstructor>();
//        constructors.put(IFunction.BuiltInFunction.INTERVAL, new DateIntervalFunction());
//        constructors.put(IFunction.BuiltInFunction.GET_FORMAT, new GetFormat());
//        constructors.put(IFunction.BuiltInFunction.TIMESTAMPADD, new TimestampAdd());
//        constructors.put(IFunction.BuiltInFunction.TIMESTAMPDIFF, new TimestampDiff());
//        constructors.put(IFunction.BuiltInFunction.CAST, new Cast());
//        constructors.put(IFunction.BuiltInFunction.CASE_WHEN, new CaseWhen());
//        constructors.put(OPERATION.EQ_ANY.getOPERATIONString(), new EqualAny());
//        constructors.put(OPERATION.EQ_ALL.getOPERATIONString(), new EqualAll());
//    }
//
//    public FunctionStringConstructor getConstructor(IFunction func) {
//        if (constructors.containsKey(func.getFunctionName())) return constructors.get(func.getFunctionName());
//        else return null;
//    }
// }
