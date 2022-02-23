package ru.mail.polis;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import ru.mail.polis.test.DaoFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest
@ArgumentsSource(DaoTest.DaoList.class)
@Timeout(5)
public @interface DaoTest {

    class DaoList implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
            CodeSource codeSource = DaoFactory.class.getProtectionDomain().getCodeSource();
            Path path = Path.of(codeSource.getLocation().toURI());
            try (Stream<Path> walk = Files.walk(path)) {
                List<Arguments> tmpList = walk.filter(p -> p.getFileName().toString().endsWith(".class")).map(p -> {
                    StringBuilder result = new StringBuilder();
                    for (Path subPath : path.relativize(p)) {
                        result.append(subPath).append(".");
                    }
                    String className = result.substring(0, result.length() - ".class.".length());
                    Class<?> clazz;
                    try {
                        clazz = Class.forName(className, false, DaoFactory.class.getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    DaoFactory factory = clazz.getAnnotation(DaoFactory.class);
                    if (factory != null) {
                        try {
                            DaoFactory.Factory<?, ?> f = (DaoFactory.Factory<?, ?>) clazz.getDeclaredConstructor().newInstance();
                            return Arguments.of(f.createStringDao());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return null;
                }).filter(Objects::nonNull).toList();
                return tmpList.stream();
            }
        }
    }

}
