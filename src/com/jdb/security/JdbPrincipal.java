/*
        Copyright 2011, Sumeet Chhetri 
  
    Licensed under the Apache License, Version 2.0 (the "License"); 
    you may not use this file except in compliance with the License. 
    You may obtain a copy of the License at 
  
        http://www.apache.org/licenses/LICENSE-2.0 
  
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
    See the License for the specific language governing permissions and 
    limitations under the License.  
*/
package com.jdb.security;

import java.security.Principal;


public class JdbPrincipal implements Principal, java.io.Serializable 
{
    
    private String name;

    
    public JdbPrincipal(String name) {
	if (name == null)
	    throw new NullPointerException("illegal null input");

	this.name = name;
    }

   
    public String getName() {
	return name;
    }

   
    public String toString() {
	return("SamplePrincipal:  " + name);
    }

   
    public boolean equals(Object o) {
	if (o == null)
	    return false;

        if (this == o)
            return true;
 
        if (!(o instanceof JdbPrincipal))
            return false;
        JdbPrincipal that = (JdbPrincipal)o;

	if (this.getName().equals(that.getName()))
	    return true;
	return false;
    }
 
   
    public int hashCode() {
	return name.hashCode();
    }
}

