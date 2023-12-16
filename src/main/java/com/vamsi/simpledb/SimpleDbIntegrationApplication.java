package com.vamsi.simpledb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.PollerFactory;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

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
        jdbcPollingChannelAdapter.setUpdatePerRow(true);
        jdbcPollingChannelAdapter.setUpdateSql("update customer set processed = true where id = :id");
        jdbcPollingChannelAdapter.setUpdateSqlParameterSourceFactory(input -> {
            if (input instanceof Customer customer) {
                return new MapSqlParameterSource("id", customer.id());
            } else {
                return null;
            }
        });
        return jdbcPollingChannelAdapter;
    }

    @Bean
    IntegrationFlow jdbcInboundFlow(JdbcPollingChannelAdapter jdbcPollingChannelAdapter) {
        return IntegrationFlow.from(jdbcPollingChannelAdapter,
                        poller -> poller.poller(pm -> PollerFactory.fixedRate(Duration.ofSeconds(1))))
                .handle((GenericHandler<List<Customer>>) (payload, headers) -> {
                    System.out.println(payload);
                    headers.forEach((k, v) -> System.out.println(k + "=" + v));
                    return null;
                })
                .get();
    }


    @Component
    static class CustomerRowMapper implements RowMapper<Customer> {
        @Override
        public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Customer(rs.getInt("id"), rs.getString("name"));
        }
    }

    record Customer(Integer id, String name) {

    }
}
