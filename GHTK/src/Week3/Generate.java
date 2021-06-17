package Week3;

import au.com.anthonybruno.Gen;
import au.com.anthonybruno.generator.defaults.IntGenerator;
import com.github.javafaker.Faker;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Generate {
    public static void main(String[] args) {
        Date now = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date then = new Date(now.getTime() + 300000000);
        Date then2 = new Date(then.getTime()+300000000);
        Faker faker = Faker.instance();
        Gen.start()
                .addField("shop_code", () -> faker.code().imei())
                .addField("customer_tel", () -> faker.phoneNumber().phoneNumber())
                .addField("customer_tel_normalize", () -> faker.phoneNumber().phoneNumber())
                .addField("fullname", () -> faker.name().fullName())
                .addField("pkg_created", () -> formatter.format(faker.date().between(now, then)))
                .addField("pkg_modified", () -> formatter.format(faker.date().between(then, then2)))
                .addField("package_status_id", () -> faker.number().numberBetween(1,100))
                .addField("customer_province_id", () -> faker.number().numberBetween(1,100))
                .addField("customer_district_id", () -> faker.number().numberBetween(1,300))
                .addField("customer_ward_id", () -> faker.number().numberBetween(1,300))
                .addField("created", () -> formatter.format(faker.date().between(now, then)))
                .addField("modified", () -> formatter.format(faker.date().between(then, then2)))
                .addField("is_cancel", new IntGenerator(0,1))
                .addField("ightk_user_id", () -> faker.number().numberBetween(1,1000000000))
                .generate(5000000)
                .asCsv()
                .toFile("ghtk.csv");
    }
}
