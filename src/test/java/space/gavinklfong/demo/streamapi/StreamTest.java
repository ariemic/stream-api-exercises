package space.gavinklfong.demo.streamapi;

import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import lombok.extern.slf4j.Slf4j;
import space.gavinklfong.demo.streamapi.models.Customer;
import space.gavinklfong.demo.streamapi.models.Order;
import space.gavinklfong.demo.streamapi.models.Product;
import space.gavinklfong.demo.streamapi.repos.CustomerRepo;
import space.gavinklfong.demo.streamapi.repos.OrderRepo;
import space.gavinklfong.demo.streamapi.repos.ProductRepo;

@Slf4j
@DataJpaTest
public class StreamTest {

    @Autowired
    private CustomerRepo customerRepo;

    @Autowired
    private OrderRepo orderRepo;

    @Autowired
    private ProductRepo productRepo;

    @Test
    @DisplayName("Obtain a list of product with category = \"Books\" and price > 100")
    public void exercise1() {
        long startTime = System.currentTimeMillis();

        List<Product> result = productRepo.findAll().stream()
                .filter(product ->
                    product.getCategory().equals("Books") &&
                    product.getPrice() > 100
                )
                .collect(Collectors.toList());

        long endTime = System.currentTimeMillis();

        log.info(String.format("exercise 1 - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(p -> log.info(p.toString()));
    }


    @Test
    @DisplayName("Obtain a list of product with category = \"Books\" and price > 100 (using Predicate chaining for filter)")
    public void exercise1a() {
        Predicate<Product> categoryFilter = product -> product.getCategory().equalsIgnoreCase("Books");
        Predicate<Product> priceFilter = product -> product.getPrice() > 100;
        long startTime = System.currentTimeMillis();
        List<Product> result =productRepo.findAll().stream()
                .filter(categoryFilter)
                .filter(priceFilter)
                .collect(Collectors.toList());

        long endTime = System.currentTimeMillis();

        log.info(String.format("exercise 1a - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(p -> log.info(p.toString()));
    }

    @Test
    @DisplayName("Obtain a list of product with category = \"Books\" and price > 100 (using BiPredicate for filter)")
    public void exercise1b() {
        BiPredicate<Product, String> categoryFilter = (product, category) -> product.getCategory().equalsIgnoreCase(category);

        long startTime = System.currentTimeMillis();
        List<Product> result = productRepo.findAll().stream()
                .filter(product -> categoryFilter.test(product, "Books") && product.getPrice() > 100)
                .collect(Collectors.toList());
        long endTime = System.currentTimeMillis();

        log.info(String.format("exercise 1b - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(p -> log.info(p.toString()));
    }

    @Test
    @DisplayName("Obtain a list of order with product category = \"Baby\"")
    public void exercise2() {
        long startTime = System.currentTimeMillis();
        List<Order> result = orderRepo.findAll().stream()
                .filter(order -> order.getProducts()
                        .stream().filter(p -> p.getCategory().equalsIgnoreCase("Baby")).isParallel())
                .collect(Collectors.toList());

        long endTime = System.currentTimeMillis();

        log.info(String.format("exercise 2 - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(o -> log.info(o.toString()));

    }

    @Test
    @DisplayName("Obtain a list of product with category = “Toys” and then apply 10% discount\"")
    public void exercise3() {
        long startTime = System.currentTimeMillis();

        List<Product> result = productRepo.findAll().stream()
                .filter(p -> p.getCategory().equals("Toys"))
                .map(p -> {
                    p.setPrice(p.getPrice() * 0.9);
                    return p;
                })
                .collect(Collectors.toList());


        long endTime = System.currentTimeMillis();
        log.info(String.format("exercise 3 - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(o -> log.info(o.toString()));

    }

    @Test
    @DisplayName("Obtain a list of products ordered by customer of tier 2 between 01-Feb-2021 and 01-Apr-2021")
    public void exercise4() {
        long startTime = System.currentTimeMillis();
        List<Product> result = orderRepo.findAll().stream()
                .filter(o -> o.getCustomer().getTier() == 2 )
                .filter(o -> o.getOrderDate().isAfter(LocalDate.of(2021,2, 1)) &&
                        o.getOrderDate().isBefore(LocalDate.of(2021, 4, 1)))
                .flatMap(o -> o.getProducts().stream())
                .distinct()
                .collect(Collectors.toList());

        long endTime = System.currentTimeMillis();
        log.info(String.format("exercise 4 - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(o -> log.info(o.toString()));
    }

    @Test
    @DisplayName("Get the 3 cheapest products of \"Books\" category")
    public void exercise5() {
        long startTime = System.currentTimeMillis();

        List<Product> result =  productRepo.findAll().stream()
                .filter(p -> p.getCategory().equals("Books"))
                .sorted(Comparator.comparing(Product::getPrice))
                .limit(3)
                .collect(Collectors.toList());






        long endTime = System.currentTimeMillis();
        log.info(String.format("exercise 5 - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(o -> log.info(o.toString()));

    }


    @Test
    @DisplayName("Get the 3 most recent placed order")
    public void exercise6() {
        long startTime = System.currentTimeMillis();
        List<Order> result = orderRepo.findAll().stream()
                .sorted(Comparator.comparing(Order::getOrderDate).reversed())
                .limit(3)
                .collect(Collectors.toList());


        long endTime = System.currentTimeMillis();
        log.info(String.format("exercise 6 - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(o -> log.info(o.toString()));
    }


    @Test
    @DisplayName("Get a list of products which was ordered on 15-Mar-2021")
    public void exercise7() {
        long startTime = System.currentTimeMillis();
        List<Product> result = orderRepo.findAll()
                .stream()
                .filter(o -> o.getOrderDate().isEqual(LocalDate.of(2021, 3, 15)))
                .peek(o -> System.out.println(o.toString()))
                .flatMap(o -> o.getProducts().stream())
                .collect(Collectors.toList());

        long endTime = System.currentTimeMillis();
        log.info(String.format("exercise 7 - execution time: %1$d ms", (endTime - startTime)));
        result.forEach(o -> log.info(o.toString()));
    }


    @Test
    @DisplayName("Calculate the total lump of all orders placed in Feb 2021")
    public void exercise8() {
        long startTime = System.currentTimeMillis();
        double result = orderRepo.findAll()
                .stream()
                .filter(o -> o.getOrderDate().isAfter(LocalDate.of(2021, 1, 31)) &&
                        o.getOrderDate().isBefore(LocalDate.of(2021, 3, 1)))
                .flatMap(o -> o.getProducts().stream())
                .mapToDouble(Product::getPrice)
                .peek(System.out::println)
                .sum();

        long endTime = System.currentTimeMillis();
        log.info(String.format("exercise 8 - execution time: %1$d ms", (endTime - startTime)));
        log.info("Total lump sum = " + result);
    }

    @Test
    @DisplayName("Calculate the total lump of all orders placed in Feb 2021 (using reduce with BiFunction)")
    public void exercise8a() {
        BiFunction<Double, Product, Double> accumulator = (acc, product) -> acc + product.getPrice();

        long startTime = System.currentTimeMillis();
        double result = orderRepo.findAll()
                .stream()
                .filter(o -> o.getOrderDate().isAfter(LocalDate.of(2021, 1, 31)) &&
                        o.getOrderDate().isBefore(LocalDate.of(2021, 3, 1)))
                .flatMap(o -> o.getProducts().stream())
                .reduce(0.0, accumulator, Double::sum);

        long endTime = System.currentTimeMillis();
        log.info(String.format("exercise 8a - execution time: %1$d ms", (endTime - startTime)));
        log.info("Total lump sum = " + result);
    }


}
