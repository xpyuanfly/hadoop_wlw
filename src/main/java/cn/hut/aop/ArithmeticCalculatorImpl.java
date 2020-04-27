package cn.hut.aop;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service("ArithmeticCalculator111")
public class ArithmeticCalculatorImpl implements ArithmeticCalculator{

    public ArithmeticCalculatorImpl(){
        super();
    }

    @Override
    public int add(int i, int j) {
        int result = i + j;
        return result;
    }

    @Override
    public int sub(int i, int j) {
        int result = i - j;
        return result;
    }

    @Override
    public int mul(int i, int j) {
        int result = i * j;
        return result;
    }

    @Override
    public int div(int i, int j) {
        int result = i /j; 
        return result;
    }
    

}