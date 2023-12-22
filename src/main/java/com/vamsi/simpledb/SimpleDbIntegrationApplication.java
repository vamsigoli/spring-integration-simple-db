package com.vamsi.simpledb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.PollerFactory;
import org.springframework.integration.jdbc.JdbcMessageHandler;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.integration.jdbc.MessagePreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

@SpringBootApplication
public class SimpleDbIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimpleDbIntegrationApplication.class, args);
    }


    @Bean
    JdbcPollingChannelAdapter jdbcPollingChannelAdapter(CustomerRowMapper customerRowMapper, DataSource dataSource) {
        var jdbcPollingChannelAdapter = new JdbcPollingChannelAdapter(dataSource,
                "SELECT * FROM CUSTOMER where processed = false");
        jdbcPollingChannelAdapter.setRowMapper(customerRowMapper);
        //the below is not optimal because we are updating the record as soon as we get as part of inbound flow.
        //it should be done as part of the outbound flow after the processing has been done in the handle methods.
//        jdbcPollingChannelAdapter.setUpdatePerRow(true);
//        jdbcPollingChannelAdapter.setUpdateSql("update customer set processed = true where id = :id");
//        jdbcPollingChannelAdapter.setUpdateSqlParameterSourceFactory(input -> {
//            if (input instanceof Customer customer) {
//                return new MapSqlParameterSource("id", customer.id());
//            } else {
//                return null;
//            }
//        });
        return jdbcPollingChannelAdapter;
    }

    @Bean
    IntegrationFlow jdbcFlow(JdbcPollingChannelAdapter jdbcPollingChannelAdapter,
                             JdbcMessageHandler outboundHandler) {
        return IntegrationFlow.from(jdbcPollingChannelAdapter,
                        poller -> poller.poller(pm -> PollerFactory.fixedRate(Duration.ofSeconds(1))))
                .split()
                .handle((payload, headers) -> {
                    System.out.println(payload);
                    headers.forEach((k, v) -> System.out.println(k + "=" + v));
                    return payload;
                })
                .aggregate()
                //if the aggregate call is present above, all the individual customer records available after split
                //pass through the handle method which prints the payload and return the same payload
                //each individual payload represents one element in the collection returned by jdbcPollingChannelAdapter
                //they are combined and one collection of customers by aggregate method and is sent to the outboundHandler
                //if you see the javadoc of JdbcMessageHandler, it says that if it receives an Iterable, it uses batch update
                //and if it receives individual object, it uses non batch update functionality. so the above aggregate
                //improves performance by using batch update
                .handle(outboundHandler)
                .get();
    }

    @Bean
    JdbcMessageHandler jdbcMessageHandler(DataSource dataSource) {
        JdbcMessageHandler jdbcMessageHandler = new JdbcMessageHandler(dataSource,
                "UPDATE CUSTOMER SET processed = true where id = ?");
        MessagePreparedStatementSetter messagePreparedStatementSetter = (ps,message) -> {
            Customer customer = (Customer) message.getPayload();
            ps.setInt(1, customer.id());
        };
        jdbcMessageHandler.setPreparedStatementSetter(messagePreparedStatementSetter);

        return jdbcMessageHandler;
    }

    @Component
    static class CustomerRowMapper implements RowMapper<Customer> {
        @Override
        public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Customer(rs.getInt("id"), rs.getString("name"));
        }
    }

    record Customer(Integer id, String name) {}
}
