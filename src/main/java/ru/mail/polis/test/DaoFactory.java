package ru.mail.polis.test;

import ru.mail.polis.BaseEntry;
import ru.mail.polis.Dao;
import ru.mail.polis.Entry;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Iterator;

@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface DaoFactory {

    interface Factory<Data, E extends Entry<Data>> {
        Dao<Data, E> createDao();
        String toString(Data data);
        Data fromString(String data);
        E fromBaseEntry(Entry<Data> baseEntry);

        default Dao<String, Entry<String>> createStringDao() {
            Dao<Data, E> delegate = createDao();
            return new Dao<>() {
                @Override
                public Iterator<Entry<String>> get(String from, String to) {
                    Iterator<E> iterator = delegate.get(fromString(from), fromString(to));
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public Entry<String> next() {
                            E next = iterator.next();
                            String key = Factory.this.toString(next.key());
                            String value = Factory.this.toString(next.value());
                            return new BaseEntry<>(key, value);
                        }
                    };
                }

                @Override
                public void upsert(Entry<String> entry) {
                    BaseEntry<Data> e = new BaseEntry<>(
                            fromString(entry.key()),
                            fromString(entry.value())
                    );
                    delegate.upsert(fromBaseEntry(e));
                }

                @Override
                public String toString() {
                    return "StringDaoFactory<" + delegate.getClass().getSimpleName() + ">";
                }
            };
        }
    }

}
