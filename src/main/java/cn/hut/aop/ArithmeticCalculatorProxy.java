package cn.hut.aop;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

public class ArithmeticCalculatorProxy {

    private ArithmeticCalculator target;
    private ArithmeticCalculator proxy = null;

    public ArithmeticCalculatorProxy(ArithmeticCalculator target){
        super();
        this.target = target;
        this.proxy = this.build();
    }

    public ArithmeticCalculator getProxy(){
        return this.proxy;
    }

    /// 把待装修的target素颜 ===>装饰成为 proxy美颜 target ==> proxy
    private ArithmeticCalculator build(){
        ArithmeticCalculator proxy = null;
        /// 中间就是装修的过 程了
        // 拆开这个素颜的
        ClassLoader loader = target.getClass().getClassLoader();
        Class [] interfaces  = new Class [] {ArithmeticCalculator.class};
        InvocationHandler h = new InvocationHandler(){
        
            @Override// add sub
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                // TODO Auto-generated method stub
                String methodName = method.getName();

                // 装修的部分
                System.out.println("[before] the methord"+ methodName + "args:" + Arrays.asList(args));

                Object result = null;

                /// 做加减乘除计算
                try {
                    result = method.invoke(target, args);
                    
                } catch (Exception e) {
                    //TODO: handle exception
                    e.printStackTrace();
                }

                // 装修的部分
                System.out.println("[after] the result:" + result);
                return result;
            }
        };

        // 合并target 形成proxy
        proxy = (ArithmeticCalculator)Proxy.newProxyInstance(loader, interfaces, h);
        return proxy;
    }

}