package com.cy.common.sink;

import com.cy.common.entity.Order;
import com.cy.common.entity.OrderDetail;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Sink2MySQLOrderDetail extends RichSinkFunction<OrderDetail> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "replace into order_detail (\n" +
                "    orderId ,\n" +
                "    cityId , \n" +
                "    cityName ,\n" +
                "    goodsId , \n" +
                "    userId ,\n" +
                "    price ,\n" +
                "    createTime \n" +
                ")  values(?, ?, ?, ?, ?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(OrderDetail value, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setString(1, value.getOrderId());
        ps.setInt(2, value.getCityId());
        ps.setString(3, value.getCityName());
        ps.setInt(4, value.getGoodsId());
        ps.setString(5, value.getUserId());
        ps.setInt(6, value.getPrice());
        ps.setInt(7, value.getCreateTime());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/world", "root", "******");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
