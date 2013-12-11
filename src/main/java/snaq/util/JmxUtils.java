/*
  ---------------------------------------------------------------------------
  DBPool : Java Database Connection Pooling <http://www.snaq.net/>
  Copyright (c) 2001-2013 Giles Winstanley. All Rights Reserved.

  This is file is part of the DBPool project, which is licensed under
  the BSD-style licence terms shown below.
  ---------------------------------------------------------------------------
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

  1. Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

  3. The name of the author may not be used to endorse or promote products
  derived from this software without specific prior written permission.

  4. Redistributions of modified versions of the source code, must be
  accompanied by documentation detailing which parts of the code are not part
  of the original software.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER "AS IS" AND ANY EXPRESS OR
  IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
  OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  ---------------------------------------------------------------------------
 */
package snaq.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import javax.management.Descriptor;
import javax.management.MBeanParameterInfo;
import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.ModelMBean;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import javax.management.modelmbean.ModelMBeanInfo;
import javax.management.modelmbean.ModelMBeanInfoSupport;
import javax.management.modelmbean.ModelMBeanOperationInfo;
import javax.management.modelmbean.RequiredModelMBean;
import snaq.util.ObjectPool;
import snaq.util.Reusable;

/**
 * Utility class for providing ObjectPool JMX support.
 *
 * @author Giles Winstanley
 */
public class JmxUtils
{
  /**
   * Creates a {@code ModelMBean} for the specified {@link ObjectPool}.
   * @param <T> object type of pool for MBean
   * @param pool pool for which to create MBean
   * @return A {@code ModelMBean} for the specified {@link ObjectPool}
   * @throws Exception
   */
  public static <T extends Reusable> ModelMBean createObjectPoolMBean(ObjectPool<T> pool) throws Exception
  {
    List<ModelMBeanAttributeInfo> ai = new ArrayList<>();
    List<ModelMBeanOperationInfo> oi = new ArrayList<>();

    String name = null;
    String desc = null;
    Method method = null;
    Descriptor ds = null;

    name = "flush";
    method = pool.getClass().getMethod("flush", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=operation" });
    oi.add(new ModelMBeanOperationInfo(method.getName(), method, ds));

    name = "setParameters";
    method = pool.getClass().getMethod("setParameters", new Class[]{ int.class, int.class, int.class, long.class });
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=operation" });
    MBeanParameterInfo[] pi = new MBeanParameterInfo[] {
      new MBeanParameterInfo("minPool", "int", "minPool"),
      new MBeanParameterInfo("maxPool", "int", "maxPool"),
      new MBeanParameterInfo("maxSize", "int", "maxSize"),
      new MBeanParameterInfo("idleTimeout", "long", "idleTimeout")
    };
    oi.add(new ModelMBeanOperationInfo(name, method.getName(), pi, "void", ModelMBeanOperationInfo.ACTION, ds));

    name = "name";
    desc = name;
    method = pool.getClass().getMethod("getName", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "minPool";
    desc = name;
    method = pool.getClass().getMethod("getMinPool", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "maxPool";
    desc = name;
    method = pool.getClass().getMethod("getMaxPool", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "maxSize";
    desc = name;
    method = pool.getClass().getMethod("getMaxSize", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "idleTimeout";
    desc = name;
    method = pool.getClass().getMethod("getIdleTimeout", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "size";
    desc = name;
    method = pool.getClass().getMethod("getSize", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "freeCount";
    desc = name;
    method = pool.getClass().getMethod("getFreeCount", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "checkedOut";
    desc = name;
    method = pool.getClass().getMethod("getCheckedOut", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "poolHitRate";
    desc = name;
    method = pool.getClass().getMethod("getPoolHitRate", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    name = "poolMissRate";
    desc = name;
    method = pool.getClass().getMethod("getPoolMissRate", new Class[]{});
    ds = new DescriptorSupport(new String[] { "name=" + name, "descriptorType=attribute", "getMethod=" + method.getName() });
    ai.add(new ModelMBeanAttributeInfo(name, desc, method, null, ds));
    oi.add(new ModelMBeanOperationInfo(method.getName(), method));

    // Create MBean.
    ModelMBeanInfo mbi = new ModelMBeanInfoSupport(
      pool.getClass().getName(),
      pool.toString(),
      ai.toArray(new ModelMBeanAttributeInfo[0]),
      null,
      oi.toArray(new ModelMBeanOperationInfo[0]),
      null
    );
    ModelMBean mmb = new RequiredModelMBean(mbi);
    mmb.setManagedResource(pool, "ObjectReference");
    return mmb;
  }
}
