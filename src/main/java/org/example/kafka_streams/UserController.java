package org.example.kafka_streams;

import lombok.RequiredArgsConstructor;
import org.example.kafka_streams.model.User;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final KafkaTemplate<String, User> kafkaTemplate;

    @PostMapping
    public String sendUser(@RequestBody User user) {
        kafkaTemplate.send("users-input", user.getName(), user);
        return "User sent: " + user.getName() + " with balance: " + user.getBalance();
    }

    @GetMapping("/test")
    public String sendTestUsers() {
        User positiveUser = new User("John", "+1234567890", 1500.50);
        User negativeUser = new User("Jane", "+0987654321", -500.0);
        User zeroUser = new User("Bob", "+1112223333", 0.0);

        kafkaTemplate.send("users-input", positiveUser.getName(), positiveUser);
        kafkaTemplate.send("users-input", negativeUser.getName(), negativeUser);
        kafkaTemplate.send("users-input", zeroUser.getName(), zeroUser);

        return "Test users sent! Check logs for results.";
    }
}