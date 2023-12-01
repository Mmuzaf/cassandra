/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.virtual.proc;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.ExecutableType;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SupportedAnnotationTypes("org.apache.cassandra.db.virtual.proc.Column")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class SystemViewAnnotationProcessor extends AbstractProcessor
{
    private static final Set<String> SYS_METHODS = new HashSet<>(Arrays.asList("equals", "hashCode", "toString",
            "getClass"));
    private static final String TAB = "    ";
    private static final String WALKER_SUFFIX = "Walker";
    private static final Pattern DOLLAR_PATTERN = Pattern.compile("\\$");
    private static final Map<String, Class<?>> namePrimitiveMap = new HashMap<>();
    private static final Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<>();

    static
    {
        namePrimitiveMap.put("boolean", Boolean.TYPE);
        namePrimitiveMap.put("byte", Byte.TYPE);
        namePrimitiveMap.put("char", Character.TYPE);
        namePrimitiveMap.put("short", Short.TYPE);
        namePrimitiveMap.put("int", Integer.TYPE);
        namePrimitiveMap.put("long", Long.TYPE);
        namePrimitiveMap.put("double", Double.TYPE);
        namePrimitiveMap.put("float", Float.TYPE);
        namePrimitiveMap.put("void", Void.TYPE);
        primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
        primitiveWrapperMap.put(Byte.TYPE, Byte.class);
        primitiveWrapperMap.put(Character.TYPE, Character.class);
        primitiveWrapperMap.put(Short.TYPE, Short.class);
        primitiveWrapperMap.put(Integer.TYPE, Integer.class);
        primitiveWrapperMap.put(Long.TYPE, Long.class);
        primitiveWrapperMap.put(Double.TYPE, Double.class);
        primitiveWrapperMap.put(Float.TYPE, Float.class);
        primitiveWrapperMap.put(Void.TYPE, Void.TYPE);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv)
    {
        for (TypeElement annotation : annotations)
        {
            Set<? extends Element> annotatedElements = roundEnv.getElementsAnnotatedWith(annotation);

//                .filter(m -> !m.getModifiers().contains(Modifier.STATIC)))
//                .filter(m -> !SYS_METHODS.contains(m.getName()))
//                .filter(m -> m.getReturnType() != void.class)
            Map<Boolean, List<Element>> annotatedMethods = annotatedElements.stream()
                    .collect(Collectors.partitioningBy(element ->
                            (((ExecutableType) element.asType()).getParameterTypes().size() > 0) ||
                                    ((ExecutableType) element.asType()).getReturnType().toString().equals("void")));

            List<Element> otherMethods = annotatedMethods.get(true);
            List<Element> getters = annotatedMethods.get(false);

            otherMethods.forEach(element -> processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "@Column must be applied to a method without an argument and can't have void type", element));

            if (getters.isEmpty())
                continue;

            Map<String, List<Element>> gettersByClass = getters.stream()
                    .collect(Collectors.groupingBy(element -> ((TypeElement) element.getEnclosingElement()).getQualifiedName().toString()));


            for (Map.Entry<String, List<Element>> classEntry : gettersByClass.entrySet())
            {
                String className = classEntry.getKey();
                Collection<String> code = generate(className, classEntry.getValue());
                try
                {
                    JavaFileObject builderFile = processingEnv.getFiler().createSourceFile(className + WALKER_SUFFIX);
                    try (PrintWriter writer = new PrintWriter(builderFile.openWriter()))
                    {
                        for (String line : code)
                        {
                            writer.write(line);
                            writer.write('\n');
                        }
                    }
                } catch (IOException e)
                {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, e.getMessage());
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Generates {@link RowWalker} implementation.
     */
    private Collection<String> generate(String className, List<Element> columns)
    {
        final List<String> code = new ArrayList<>();
        final Set<String> imports = new TreeSet<>(Comparator.comparing(s -> s.replace(";", "")));
        String packageName =  className.substring(0,  className.lastIndexOf('.'));
        String simpleClassName = className.substring(className.lastIndexOf('.') + 1);

        addImport(imports, RowWalker.class.getName());
        addImport(imports, className);

        code.add("package " + packageName + ';');
        code.add("");
        code.add("");
        code.add("/**");
        code.add(" * Generated by {@code " + SystemViewAnnotationProcessor.class.getName() + "}.");
        code.add(" * {@link " + simpleClassName + "} row metadata and data walker.");
        code.add(" * ");
        code.add(" * @see " + simpleClassName);
        code.add(" */");
        code.add("public class " + simpleClassName + WALKER_SUFFIX + " implements RowWalker<" + simpleClassName + '>');
        code.add("{");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitMeta(RowWalker.MetadataVisitor visitor)");
        code.add(TAB + '{');

        forEachColumn(columns, (method, index) -> {
            String name = method.getSimpleName().toString();
            String returnType = ((ExecutableType) method.asType()).getReturnType().toString();

            if (!isPrimitive(returnType) && !returnType.startsWith("java.lang"))
                addImport(imports, returnType);

            String line = TAB + TAB +
                    "visitor.accept(" + index + ", \"" + name + "\", " +
                    getPrimitiveWrapperClass(returnType) +
                    (isPrimitive(returnType) ? ".TYPE);" : ".class);");

            code.add(line);
        });

        code.add(TAB + '}');
        code.add("");
        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public void visitRow(" + simpleClassName + " row, RowWalker.RowMetadataVisitor visitor)");
        code.add(TAB + '{');

        forEachColumn(columns, (method, index) -> {
            String name = method.getSimpleName().toString();
            String returnType = ((ExecutableType) method.asType()).getReturnType().toString();
            String line = TAB + TAB +
                    "visitor.accept(" + index + ", \"" + name + "\", " +
                    getPrimitiveWrapperClass(returnType) +
                    (isPrimitive(returnType) ? ".TYPE, row." : ".class, row.") +
                    name + "());";
            code.add(line);
        });

        code.add(TAB + '}');
        code.add("");

        final int[] cnt = {0};
        forEachColumn(columns, (m, i) -> cnt[0]++);

        code.add(TAB + "/** {@inheritDoc} */");
        code.add(TAB + "@Override public int count() {");
        code.add(TAB + TAB + "return " + cnt[0] + ';');
        code.add(TAB + '}');
        code.add("}");

        code.addAll(2, imports);

        addLicenseHeader(code);

        return code;
    }

    private void addImport(Set<String> imports, String className)
    {
        imports.add("import " + DOLLAR_PATTERN.matcher(className).replaceAll(".") + ';');
    }

    private void addLicenseHeader(List<String> code)
    {
        List<String> license = new ArrayList<>();

        license.add("/*");
        license.add(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        license.add(" * contributor license agreements.  See the NOTICE file distributed with");
        license.add(" * this work for additional information regarding copyright ownership.");
        license.add(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        license.add(" * (the \"License\"); you may not use this file except in compliance with");
        license.add(" * the License.  You may obtain a copy of the License at");
        license.add(" *");
        license.add(" *      https://www.apache.org/licenses/LICENSE-2.0");
        license.add(" *");
        license.add(" * Unless required by applicable law or agreed to in writing, software");
        license.add(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        license.add(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        license.add(" * See the License for the specific language governing permissions and");
        license.add(" * limitations under the License.");
        license.add(" */");
        license.add("");
        code.addAll(0, license);
    }

    /**
     * Iterates each over the {@code columns} and consumes {@code method} for it.
     */
    private static void forEachColumn(List<Element> columns, ObjIntConsumer<Element> consumer)
    {
        columns.stream()
                .sorted(Comparator.comparingInt(o -> o.getAnnotation(Column.class).index()))
                .forEach(method -> consumer.accept(method, method.getAnnotation(Column.class).index()));
    }

    public static boolean isPrimitive(String className)
    {
        return namePrimitiveMap.containsKey(className);
    }

    public static String getPrimitiveWrapperClass(String className)
    {
        return isPrimitive(className) ?
                primitiveWrapperMap.get(namePrimitiveMap.get(className)).getSimpleName() : className;
    }
}
