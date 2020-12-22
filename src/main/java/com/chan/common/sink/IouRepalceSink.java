package com.chan.common.sink;

import com.chan.common.entity.Iou;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class IouRepalceSink extends RichSinkFunction<Iou> {
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
        String sql = "replace into iou (\n" +
                "    order_id ,\n" +
                "    down_payment , \n" +
                "    order_amt ,\n" +
                "    update_time , \n" +
                "    etl_time \n" +
                ")  values(?, ?, ?, ?, null);";
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
    public void invoke(Iou value, Context context) throws Exception {

        //组装数据，执行插入操作
        ps.setLong(1, value.getOrder_id());
        ps.setObject(2, value.getDown_Payment());
        ps.setObject(3, value.getOrder_amt());
        ps.setObject(4, value.getUpdate_time());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/world", "root", "******");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
