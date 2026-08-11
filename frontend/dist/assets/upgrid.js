(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const s of document.querySelectorAll('link[rel="modulepreload"]'))n(s);new MutationObserver(s=>{for(const a of s)if(a.type==="childList")for(const r of a.addedNodes)r.tagName==="LINK"&&r.rel==="modulepreload"&&n(r)}).observe(document,{childList:!0,subtree:!0});function i(s){const a={};return s.integrity&&(a.integrity=s.integrity),s.referrerPolicy&&(a.referrerPolicy=s.referrerPolicy),s.crossOrigin==="use-credentials"?a.credentials="include":s.crossOrigin==="anonymous"?a.credentials="omit":a.credentials="same-origin",a}function n(s){if(s.ep)return;s.ep=!0;const a=i(s);fetch(s.href,a)}})();const ie=globalThis,Ie=ie.ShadowRoot&&(ie.ShadyCSS===void 0||ie.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Oe=Symbol(),Je=new WeakMap;let vt=class{constructor(e,i,n){if(this._$cssResult$=!0,n!==Oe)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=i}get styleSheet(){let e=this.o;const i=this.t;if(Ie&&e===void 0){const n=i!==void 0&&i.length===1;n&&(e=Je.get(i)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),n&&Je.set(i,e))}return e}toString(){return this.cssText}};const Wt=t=>new vt(typeof t=="string"?t:t+"",void 0,Oe),je=(t,...e)=>{const i=t.length===1?t[0]:e.reduce((n,s,a)=>n+(r=>{if(r._$cssResult$===!0)return r.cssText;if(typeof r=="number")return r;throw Error("Value passed to 'css' function must be a 'css' function result: "+r+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(s)+t[a+1],t[0]);return new vt(i,t,Oe)},Qt=(t,e)=>{if(Ie)t.adoptedStyleSheets=e.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of e){const n=document.createElement("style"),s=ie.litNonce;s!==void 0&&n.setAttribute("nonce",s),n.textContent=i.cssText,t.appendChild(n)}},Be=Ie?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let i="";for(const n of e.cssRules)i+=n.cssText;return Wt(i)})(t):t;const{is:Yt,defineProperty:Zt,getOwnPropertyDescriptor:Xt,getOwnPropertyNames:ei,getOwnPropertySymbols:ti,getPrototypeOf:ii}=Object,he=globalThis,Ke=he.trustedTypes,si=Ke?Ke.emptyScript:"",ni=he.reactiveElementPolyfillSupport,B=(t,e)=>t,re={toAttribute(t,e){switch(e){case Boolean:t=t?si:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let i=t;switch(e){case Boolean:i=t!==null;break;case Number:i=t===null?null:Number(t);break;case Object:case Array:try{i=JSON.parse(t)}catch{i=null}}return i}},Ne=(t,e)=>!Yt(t,e),Ve={attribute:!0,type:String,converter:re,reflect:!1,useDefault:!1,hasChanged:Ne};Symbol.metadata??=Symbol("metadata"),he.litPropertyMetadata??=new WeakMap;let N=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,i=Ve){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(e,i),!i.noAccessor){const n=Symbol(),s=this.getPropertyDescriptor(e,n,i);s!==void 0&&Zt(this.prototype,e,s)}}static getPropertyDescriptor(e,i,n){const{get:s,set:a}=Xt(this.prototype,e)??{get(){return this[i]},set(r){this[i]=r}};return{get:s,set(r){const o=s?.call(this);a?.call(this,r),this.requestUpdate(e,o,n)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??Ve}static _$Ei(){if(this.hasOwnProperty(B("elementProperties")))return;const e=ii(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(B("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(B("properties"))){const i=this.properties,n=[...ei(i),...ti(i)];for(const s of n)this.createProperty(s,i[s])}const e=this[Symbol.metadata];if(e!==null){const i=litPropertyMetadata.get(e);if(i!==void 0)for(const[n,s]of i)this.elementProperties.set(n,s)}this._$Eh=new Map;for(const[i,n]of this.elementProperties){const s=this._$Eu(i,n);s!==void 0&&this._$Eh.set(s,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const i=[];if(Array.isArray(e)){const n=new Set(e.flat(1/0).reverse());for(const s of n)i.unshift(Be(s))}else e!==void 0&&i.push(Be(e));return i}static _$Eu(e,i){const n=i.attribute;return n===!1?void 0:typeof n=="string"?n:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,i=this.constructor.elementProperties;for(const n of i.keys())this.hasOwnProperty(n)&&(e.set(n,this[n]),delete this[n]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Qt(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,i,n){this._$AK(e,n)}_$ET(e,i){const n=this.constructor.elementProperties.get(e),s=this.constructor._$Eu(e,n);if(s!==void 0&&n.reflect===!0){const a=(n.converter?.toAttribute!==void 0?n.converter:re).toAttribute(i,n.type);this._$Em=e,a==null?this.removeAttribute(s):this.setAttribute(s,a),this._$Em=null}}_$AK(e,i){const n=this.constructor,s=n._$Eh.get(e);if(s!==void 0&&this._$Em!==s){const a=n.getPropertyOptions(s),r=typeof a.converter=="function"?{fromAttribute:a.converter}:a.converter?.fromAttribute!==void 0?a.converter:re;this._$Em=s;const o=r.fromAttribute(i,a.type);this[s]=o??this._$Ej?.get(s)??o,this._$Em=null}}requestUpdate(e,i,n,s=!1,a){if(e!==void 0){const r=this.constructor;if(s===!1&&(a=this[e]),n??=r.getPropertyOptions(e),!((n.hasChanged??Ne)(a,i)||n.useDefault&&n.reflect&&a===this._$Ej?.get(e)&&!this.hasAttribute(r._$Eu(e,n))))return;this.C(e,i,n)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,i,{useDefault:n,reflect:s,wrapped:a},r){n&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,r??i??this[e]),a!==!0||r!==void 0)||(this._$AL.has(e)||(this.hasUpdated||n||(i=void 0),this._$AL.set(e,i)),s===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[s,a]of this._$Ep)this[s]=a;this._$Ep=void 0}const n=this.constructor.elementProperties;if(n.size>0)for(const[s,a]of n){const{wrapped:r}=a,o=this[s];r!==!0||this._$AL.has(s)||o===void 0||this.C(s,void 0,a,o)}}let e=!1;const i=this._$AL;try{e=this.shouldUpdate(i),e?(this.willUpdate(i),this._$EO?.forEach(n=>n.hostUpdate?.()),this.update(i)):this._$EM()}catch(n){throw e=!1,this._$EM(),n}e&&this._$AE(i)}willUpdate(e){}_$AE(e){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(e){}firstUpdated(e){}};N.elementStyles=[],N.shadowRootOptions={mode:"open"},N[B("elementProperties")]=new Map,N[B("finalized")]=new Map,ni?.({ReactiveElement:N}),(he.reactiveElementVersions??=[]).push("2.1.2");const Me=globalThis,Ge=t=>t,oe=Me.trustedTypes,We=oe?oe.createPolicy("lit-html",{createHTML:t=>t}):void 0,yt="$lit$",E=`lit$${Math.random().toFixed(9).slice(2)}$`,xt="?"+E,ai=`<${xt}>`,j=document,V=()=>j.createComment(""),G=t=>t===null||typeof t!="object"&&typeof t!="function",Re=Array.isArray,ri=t=>Re(t)||typeof t?.[Symbol.iterator]=="function",ve=`[ 	
\f\r]`,z=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,Qe=/-->/g,Ye=/>/g,I=RegExp(`>|${ve}(?:([^\\s"'>=/]+)(${ve}*=${ve}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),Ze=/'/g,Xe=/"/g,wt=/^(?:script|style|textarea|title)$/i,oi=t=>(e,...i)=>({_$litType$:t,strings:e,values:i}),d=oi(1),R=Symbol.for("lit-noChange"),h=Symbol.for("lit-nothing"),et=new WeakMap,O=j.createTreeWalker(j,129);function $t(t,e){if(!Re(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return We!==void 0?We.createHTML(e):e}const li=(t,e)=>{const i=t.length-1,n=[];let s,a=e===2?"<svg>":e===3?"<math>":"",r=z;for(let o=0;o<i;o++){const l=t[o];let c,u,p=-1,y=0;for(;y<l.length&&(r.lastIndex=y,u=r.exec(l),u!==null);)y=r.lastIndex,r===z?u[1]==="!--"?r=Qe:u[1]!==void 0?r=Ye:u[2]!==void 0?(wt.test(u[2])&&(s=RegExp("</"+u[2],"g")),r=I):u[3]!==void 0&&(r=I):r===I?u[0]===">"?(r=s??z,p=-1):u[1]===void 0?p=-2:(p=r.lastIndex-u[2].length,c=u[1],r=u[3]===void 0?I:u[3]==='"'?Xe:Ze):r===Xe||r===Ze?r=I:r===Qe||r===Ye?r=z:(r=I,s=void 0);const x=r===I&&t[o+1].startsWith("/>")?" ":"";a+=r===z?l+ai:p>=0?(n.push(c),l.slice(0,p)+yt+l.slice(p)+E+x):l+E+(p===-2?o:x)}return[$t(t,a+(t[i]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),n]};class W{constructor({strings:e,_$litType$:i},n){let s;this.parts=[];let a=0,r=0;const o=e.length-1,l=this.parts,[c,u]=li(e,i);if(this.el=W.createElement(c,n),O.currentNode=this.el.content,i===2||i===3){const p=this.el.content.firstChild;p.replaceWith(...p.childNodes)}for(;(s=O.nextNode())!==null&&l.length<o;){if(s.nodeType===1){if(s.hasAttributes())for(const p of s.getAttributeNames())if(p.endsWith(yt)){const y=u[r++],x=s.getAttribute(p).split(E),f=/([.?@])?(.*)/.exec(y);l.push({type:1,index:a,name:f[2],strings:x,ctor:f[1]==="."?di:f[1]==="?"?ui:f[1]==="@"?pi:ge}),s.removeAttribute(p)}else p.startsWith(E)&&(l.push({type:6,index:a}),s.removeAttribute(p));if(wt.test(s.tagName)){const p=s.textContent.split(E),y=p.length-1;if(y>0){s.textContent=oe?oe.emptyScript:"";for(let x=0;x<y;x++)s.append(p[x],V()),O.nextNode(),l.push({type:2,index:++a});s.append(p[y],V())}}}else if(s.nodeType===8)if(s.data===xt)l.push({type:2,index:a});else{let p=-1;for(;(p=s.data.indexOf(E,p+1))!==-1;)l.push({type:7,index:a}),p+=E.length-1}a++}}static createElement(e,i){const n=j.createElement("template");return n.innerHTML=e,n}}function L(t,e,i=t,n){if(e===R)return e;let s=n!==void 0?i._$Co?.[n]:i._$Cl;const a=G(e)?void 0:e._$litDirective$;return s?.constructor!==a&&(s?._$AO?.(!1),a===void 0?s=void 0:(s=new a(t),s._$AT(t,i,n)),n!==void 0?(i._$Co??=[])[n]=s:i._$Cl=s),s!==void 0&&(e=L(t,s._$AS(t,e.values),s,n)),e}class ci{constructor(e,i){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:i},parts:n}=this._$AD,s=(e?.creationScope??j).importNode(i,!0);O.currentNode=s;let a=O.nextNode(),r=0,o=0,l=n[0];for(;l!==void 0;){if(r===l.index){let c;l.type===2?c=new Z(a,a.nextSibling,this,e):l.type===1?c=new l.ctor(a,l.name,l.strings,this,e):l.type===6&&(c=new hi(a,this,e)),this._$AV.push(c),l=n[++o]}r!==l?.index&&(a=O.nextNode(),r++)}return O.currentNode=j,s}p(e){let i=0;for(const n of this._$AV)n!==void 0&&(n.strings!==void 0?(n._$AI(e,n,i),i+=n.strings.length-2):n._$AI(e[i])),i++}}class Z{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,i,n,s){this.type=2,this._$AH=h,this._$AN=void 0,this._$AA=e,this._$AB=i,this._$AM=n,this.options=s,this._$Cv=s?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&e?.nodeType===11&&(e=i.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,i=this){e=L(this,e,i),G(e)?e===h||e==null||e===""?(this._$AH!==h&&this._$AR(),this._$AH=h):e!==this._$AH&&e!==R&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):ri(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==h&&G(this._$AH)?this._$AA.nextSibling.data=e:this.T(j.createTextNode(e)),this._$AH=e}$(e){const{values:i,_$litType$:n}=e,s=typeof n=="number"?this._$AC(e):(n.el===void 0&&(n.el=W.createElement($t(n.h,n.h[0]),this.options)),n);if(this._$AH?._$AD===s)this._$AH.p(i);else{const a=new ci(s,this),r=a.u(this.options);a.p(i),this.T(r),this._$AH=a}}_$AC(e){let i=et.get(e.strings);return i===void 0&&et.set(e.strings,i=new W(e)),i}k(e){Re(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let n,s=0;for(const a of e)s===i.length?i.push(n=new Z(this.O(V()),this.O(V()),this,this.options)):n=i[s],n._$AI(a),s++;s<i.length&&(this._$AR(n&&n._$AB.nextSibling,s),i.length=s)}_$AR(e=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);e!==this._$AB;){const n=Ge(e).nextSibling;Ge(e).remove(),e=n}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class ge{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,i,n,s,a){this.type=1,this._$AH=h,this._$AN=void 0,this.element=e,this.name=i,this._$AM=s,this.options=a,n.length>2||n[0]!==""||n[1]!==""?(this._$AH=Array(n.length-1).fill(new String),this.strings=n):this._$AH=h}_$AI(e,i=this,n,s){const a=this.strings;let r=!1;if(a===void 0)e=L(this,e,i,0),r=!G(e)||e!==this._$AH&&e!==R,r&&(this._$AH=e);else{const o=e;let l,c;for(e=a[0],l=0;l<a.length-1;l++)c=L(this,o[n+l],i,l),c===R&&(c=this._$AH[l]),r||=!G(c)||c!==this._$AH[l],c===h?e=h:e!==h&&(e+=(c??"")+a[l+1]),this._$AH[l]=c}r&&!s&&this.j(e)}j(e){e===h?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class di extends ge{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===h?void 0:e}}class ui extends ge{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==h)}}class pi extends ge{constructor(e,i,n,s,a){super(e,i,n,s,a),this.type=5}_$AI(e,i=this){if((e=L(this,e,i,0)??h)===R)return;const n=this._$AH,s=e===h&&n!==h||e.capture!==n.capture||e.once!==n.once||e.passive!==n.passive,a=e!==h&&(n===h||s);s&&this.element.removeEventListener(this.name,this,n),a&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class hi{constructor(e,i,n){this.element=e,this.type=6,this._$AN=void 0,this._$AM=i,this.options=n}get _$AU(){return this._$AM._$AU}_$AI(e){L(this,e)}}const gi=Me.litHtmlPolyfillSupport;gi?.(W,Z),(Me.litHtmlVersions??=[]).push("3.3.3");const fi=(t,e,i)=>{const n=i?.renderBefore??e;let s=n._$litPart$;if(s===void 0){const a=i?.renderBefore??null;n._$litPart$=s=new Z(e.insertBefore(V(),a),a,void 0,i??{})}return s._$AI(t),s};const Le=globalThis;class M extends N{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=fi(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return R}}M._$litElement$=!0,M.finalized=!0,Le.litElementHydrateSupport?.({LitElement:M});const mi=Le.litElementPolyfillSupport;mi?.({LitElement:M});(Le.litElementVersions??=[]).push("4.2.2");const kt=t=>(e,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const bi={attribute:!0,type:String,converter:re,reflect:!1,hasChanged:Ne},vi=(t=bi,e,i)=>{const{kind:n,metadata:s}=i;let a=globalThis.litPropertyMetadata.get(s);if(a===void 0&&globalThis.litPropertyMetadata.set(s,a=new Map),n==="setter"&&((t=Object.create(t)).wrapped=!0),a.set(i.name,t),n==="accessor"){const{name:r}=i;return{set(o){const l=e.get.call(this);e.set.call(this,o),this.requestUpdate(r,l,t,!0,o)},init(o){return o!==void 0&&this.C(r,void 0,t,o),o}}}if(n==="setter"){const{name:r}=i;return function(o){const l=this[r];e.call(this,o),this.requestUpdate(r,l,t,!0,o)}}throw Error("Unsupported decorator location: "+n)};function _t(t){return(e,i)=>typeof i=="object"?vi(t,e,i):((n,s,a)=>{const r=s.hasOwnProperty(a);return s.constructor.createProperty(a,n),r?Object.getOwnPropertyDescriptor(s,a):void 0})(t,e,i)}function m(t){return _t({...t,state:!0,attribute:!1})}const St={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},Tt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},le={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},Ct={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const At=Object.freeze({left:0,top:0,width:16,height:16}),ce=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),X=Object.freeze({...At,...ce}),ke=Object.freeze({...X,body:"",hidden:!1}),yi=Object.freeze({width:null,height:null}),Et=Object.freeze({...yi,...ce});function xi(t,e=0){const i=t.replace(/^-?[0-9.]*/,"");function n(s){for(;s<0;)s+=4;return s%4}if(i===""){const s=parseInt(t);return isNaN(s)?0:n(s)}else if(i!==t){let s=0;switch(i){case"%":s=25;break;case"deg":s=90}if(s){let a=parseFloat(t.slice(0,t.length-i.length));return isNaN(a)?0:(a=a/s,a%1===0?n(a):0)}}return e}const wi=/[\s,]+/;function $i(t,e){e.split(wi).forEach(i=>{switch(i.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const Pt={...Et,preserveAspectRatio:""};function tt(t){const e={...Pt},i=(n,s)=>t.getAttribute(n)||s;return e.width=i("width",null),e.height=i("height",null),e.rotate=xi(i("rotate","")),$i(e,i("flip","")),e.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),e}function ki(t,e){for(const i in Pt)if(t[i]!==e[i])return!0;return!1}const Dt=/^[a-z0-9]+(-[a-z0-9]+)*$/,ee=(t,e,i,n="")=>{const s=t.split(":");if(t.slice(0,1)==="@"){if(s.length<2||s.length>3)return null;n=s.shift().slice(1)}if(s.length>3||!s.length)return null;if(s.length>1){const o=s.pop(),l=s.pop(),c={provider:s.length>0?s[0]:n,prefix:l,name:o};return e&&!se(c)?null:c}const a=s[0],r=a.split("-");if(r.length>1){const o={provider:n,prefix:r.shift(),name:r.join("-")};return e&&!se(o)?null:o}if(i&&n===""){const o={provider:n,prefix:"",name:a};return e&&!se(o,i)?null:o}return null},se=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function _i(t,e){const i=t.icons,n=t.aliases||Object.create(null),s=Object.create(null);function a(r){if(i[r])return s[r]=[];if(!(r in s)){s[r]=null;const o=n[r]&&n[r].parent,l=o&&a(o);l&&(s[r]=[o].concat(l))}return s[r]}return Object.keys(i).concat(Object.keys(n)).forEach(a),s}function Si(t,e){const i={};!t.hFlip!=!e.hFlip&&(i.hFlip=!0),!t.vFlip!=!e.vFlip&&(i.vFlip=!0);const n=((t.rotate||0)+(e.rotate||0))%4;return n&&(i.rotate=n),i}function it(t,e){const i=Si(t,e);for(const n in ke)n in ce?n in t&&!(n in i)&&(i[n]=ce[n]):n in e?i[n]=e[n]:n in t&&(i[n]=t[n]);return i}function Ti(t,e,i){const n=t.icons,s=t.aliases||Object.create(null);let a={};function r(o){a=it(n[o]||s[o],a)}return r(e),i.forEach(r),it(t,a)}function It(t,e){const i=[];if(typeof t!="object"||typeof t.icons!="object")return i;t.not_found instanceof Array&&t.not_found.forEach(s=>{e(s,null),i.push(s)});const n=_i(t);for(const s in n){const a=n[s];a&&(e(s,Ti(t,s,a)),i.push(s))}return i}const Ci={provider:"",aliases:{},not_found:{},...At};function ye(t,e){for(const i in e)if(i in t&&typeof t[i]!=typeof e[i])return!1;return!0}function Ot(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!ye(t,Ci))return null;const i=e.icons;for(const s in i){const a=i[s];if(!s||typeof a.body!="string"||!ye(a,ke))return null}const n=e.aliases||Object.create(null);for(const s in n){const a=n[s],r=a.parent;if(!s||typeof r!="string"||!i[r]&&!n[r]||!ye(a,ke))return null}return e}const de=Object.create(null);function Ai(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function A(t,e){const i=de[t]||(de[t]=Object.create(null));return i[e]||(i[e]=Ai(t,e))}function jt(t,e){return Ot(e)?It(e,(i,n)=>{n?t.icons[i]=n:t.missing.add(i)}):[]}function Ei(t,e,i){try{if(typeof i.body=="string")return t.icons[e]={...i},!0}catch{}return!1}function Pi(t,e){let i=[];return(typeof t=="string"?[t]:Object.keys(de)).forEach(n=>{(typeof n=="string"&&typeof e=="string"?[e]:Object.keys(de[n]||{})).forEach(s=>{const a=A(n,s);i=i.concat(Object.keys(a.icons).map(r=>(n!==""?"@"+n+":":"")+s+":"+r))})}),i}let Q=!1;function Nt(t){return typeof t=="boolean"&&(Q=t),Q}function Y(t){const e=typeof t=="string"?ee(t,!0,Q):t;if(e){const i=A(e.provider,e.prefix),n=e.name;return i.icons[n]||(i.missing.has(n)?null:void 0)}}function Mt(t,e){const i=ee(t,!0,Q);if(!i)return!1;const n=A(i.provider,i.prefix);return e?Ei(n,i.name,e):(n.missing.add(i.name),!0)}function st(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),Q&&!e&&!t.prefix){let n=!1;return Ot(t)&&(t.prefix="",It(t,(s,a)=>{Mt(s,a)&&(n=!0)})),n}const i=t.prefix;return se({prefix:i,name:"a"})?!!jt(A(e,i),t):!1}function Di(t){return!!Y(t)}function Ii(t){const e=Y(t);return e&&{...X,...e}}function Rt(t,e){t.forEach(i=>{const n=i.loaderCallbacks;n&&(i.loaderCallbacks=n.filter(s=>s.id!==e))})}function Oi(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let i=!1;const n=t.provider,s=t.prefix;e.forEach(a=>{const r=a.icons,o=r.pending.length;r.pending=r.pending.filter(l=>{if(l.prefix!==s)return!0;const c=l.name;if(t.icons[c])r.loaded.push({provider:n,prefix:s,name:c});else if(t.missing.has(c))r.missing.push({provider:n,prefix:s,name:c});else return i=!0,!0;return!1}),r.pending.length!==o&&(i||Rt([t],a.id),a.callback(r.loaded.slice(0),r.missing.slice(0),r.pending.slice(0),a.abort))})}))}let ji=0;function Ni(t,e,i){const n=ji++,s=Rt.bind(null,i,n);if(!e.pending.length)return s;const a={id:n,icons:e,callback:t,abort:s};return i.forEach(r=>{(r.loaderCallbacks||(r.loaderCallbacks=[])).push(a)}),s}function Mi(t){const e={loaded:[],missing:[],pending:[]},i=Object.create(null);t.sort((s,a)=>s.provider!==a.provider?s.provider.localeCompare(a.provider):s.prefix!==a.prefix?s.prefix.localeCompare(a.prefix):s.name.localeCompare(a.name));let n={provider:"",prefix:"",name:""};return t.forEach(s=>{if(n.name===s.name&&n.prefix===s.prefix&&n.provider===s.provider)return;n=s;const a=s.provider,r=s.prefix,o=s.name,l=i[a]||(i[a]=Object.create(null)),c=l[r]||(l[r]=A(a,r));let u;o in c.icons?u=e.loaded:r===""||c.missing.has(o)?u=e.missing:u=e.pending;const p={provider:a,prefix:r,name:o};u.push(p)}),e}const _e=Object.create(null);function nt(t,e){_e[t]=e}function Se(t){return _e[t]||_e[""]}function Ri(t,e=!0,i=!1){const n=[];return t.forEach(s=>{const a=typeof s=="string"?ee(s,e,i):s;a&&n.push(a)}),n}function Ue(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const fe=Object.create(null),H=["https://api.simplesvg.com","https://api.unisvg.com"],ne=[];for(;H.length>0;)H.length===1||Math.random()>.5?ne.push(H.shift()):ne.push(H.pop());fe[""]=Ue({resources:["https://api.iconify.design"].concat(ne)});function at(t,e){const i=Ue(e);return i===null?!1:(fe[t]=i,!0)}function me(t){return fe[t]}function Li(){return Object.keys(fe)}const Ui={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Fi(t,e,i,n){const s=t.resources.length,a=t.random?Math.floor(Math.random()*s):t.index;let r;if(t.random){let w=t.resources.slice(0);for(r=[];w.length>1;){const _=Math.floor(Math.random()*w.length);r.push(w[_]),w=w.slice(0,_).concat(w.slice(_+1))}r=r.concat(w)}else r=t.resources.slice(a).concat(t.resources.slice(0,a));const o=Date.now();let l="pending",c=0,u,p=null,y=[],x=[];typeof n=="function"&&x.push(n);function f(){p&&(clearTimeout(p),p=null)}function C(){l==="pending"&&(l="aborted"),f(),y.forEach(w=>{w.status==="pending"&&(w.status="aborted")}),y=[]}function $(w,_){_&&(x=[]),typeof w=="function"&&x.push(w)}function F(){return{startTime:o,payload:e,status:l,queriesSent:c,queriesPending:y.length,subscribe:$,abort:C}}function T(){l="failed",x.forEach(w=>{w(void 0,u)})}function S(){y.forEach(w=>{w.status==="pending"&&(w.status="aborted")}),y=[]}function k(w,_,q){const te=_!=="success";switch(y=y.filter(D=>D!==w),l){case"pending":break;case"failed":if(te||!t.dataAfterTimeout)return;break;default:return}if(_==="abort"){u=q,T();return}if(te){u=q,y.length||(r.length?be():T());return}if(f(),S(),!t.random){const D=t.resources.indexOf(w.resource);D!==-1&&D!==t.index&&(t.index=D)}l="completed",x.forEach(D=>{D(q)})}function be(){if(l!=="pending")return;f();const w=r.shift();if(w===void 0){if(y.length){p=setTimeout(()=>{f(),l==="pending"&&(S(),T())},t.timeout);return}T();return}const _={status:"pending",resource:w,callback:(q,te)=>{k(_,q,te)}};y.push(_),c++,p=setTimeout(be,t.rotate),i(w,e,_.callback)}return setTimeout(be),F}function Lt(t){const e={...Ui,...t};let i=[];function n(){i=i.filter(r=>r().status==="pending")}function s(r,o,l){const c=Fi(e,r,o,(u,p)=>{n(),l&&l(u,p)});return i.push(c),c}function a(r){return i.find(o=>r(o))||null}return{query:s,find:a,setIndex:r=>{e.index=r},getIndex:()=>e.index,cleanup:n}}function rt(){}const xe=Object.create(null);function qi(t){if(!xe[t]){const e=me(t);if(!e)return;xe[t]={config:e,redundancy:Lt(e)}}return xe[t]}function Ut(t,e,i){let n,s;if(typeof t=="string"){const a=Se(t);if(!a)return i(void 0,424),rt;s=a.send;const r=qi(t);r&&(n=r.redundancy)}else{const a=Ue(t);if(a){n=Lt(a);const r=Se(t.resources?t.resources[0]:"");r&&(s=r.send)}}return!n||!s?(i(void 0,424),rt):n.query(e,s,i)().abort}function ot(){}function zi(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,Oi(t)}))}function Hi(t){const e=[],i=[];return t.forEach(n=>{(n.match(Dt)?e:i).push(n)}),{valid:e,invalid:i}}function J(t,e,i){function n(){const s=t.pendingIcons;e.forEach(a=>{s&&s.delete(a),t.icons[a]||t.missing.add(a)})}if(i&&typeof i=="object")try{if(!jt(t,i).length){n();return}}catch(s){console.error(s)}n(),zi(t)}function lt(t,e){t instanceof Promise?t.then(i=>{e(i)}).catch(()=>{e(null)}):e(t)}function Ji(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:i,prefix:n}=t,s=t.iconsToLoad;if(delete t.iconsToLoad,!s||!s.length)return;const a=t.loadIcon;if(t.loadIcons&&(s.length>1||!a)){lt(t.loadIcons(s,n,i),c=>{J(t,s,c)});return}if(a){s.forEach(c=>{lt(a(c,n,i),u=>{J(t,[c],u?{prefix:n,icons:{[c]:u}}:null)})});return}const{valid:r,invalid:o}=Hi(s);if(o.length&&J(t,o,null),!r.length)return;const l=n.match(Dt)?Se(i):null;if(!l){J(t,r,null);return}l.prepare(i,n,r).forEach(c=>{Ut(i,c,u=>{J(t,c.icons,u)})})}))}const Fe=(t,e)=>{const i=Mi(Ri(t,!0,Nt()));if(!i.pending.length){let o=!0;return e&&setTimeout(()=>{o&&e(i.loaded,i.missing,i.pending,ot)}),()=>{o=!1}}const n=Object.create(null),s=[];let a,r;return i.pending.forEach(o=>{const{provider:l,prefix:c}=o;if(c===r&&l===a)return;a=l,r=c,s.push(A(l,c));const u=n[l]||(n[l]=Object.create(null));u[c]||(u[c]=[])}),i.pending.forEach(o=>{const{provider:l,prefix:c,name:u}=o,p=A(l,c),y=p.pendingIcons||(p.pendingIcons=new Set);y.has(u)||(y.add(u),n[l][c].push(u))}),s.forEach(o=>{const l=n[o.provider][o.prefix];l.length&&Ji(o,l)}),e?Ni(e,i,s):ot},Bi=t=>new Promise((e,i)=>{const n=typeof t=="string"?ee(t,!0):t;if(!n){i(t);return}Fe([n||t],s=>{if(s.length&&n){const a=Y(n);if(a){e({...X,...a});return}}i(t)})});function ct(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Ki(t,e){if(typeof t=="object")return{data:ct(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const a=ct(t);if(a)return{data:a,value:t}}const i=ee(t,!0,!0);if(!i)return{value:t};const n=Y(i);if(n!==void 0||!i.prefix)return{value:t,name:i,data:n};const s=Fe([i],()=>e(t,i,Y(i)));return{value:t,name:i,loading:s}}let Ft=!1;try{Ft=navigator.vendor.indexOf("Apple")===0}catch{}function Vi(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(Ft||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const Gi=/(-?[0-9.]*[0-9]+[0-9.]*)/g,Wi=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function Te(t,e,i){if(e===1)return t;if(i=i||100,typeof t=="number")return Math.ceil(t*e*i)/i;if(typeof t!="string")return t;const n=t.split(Gi);if(n===null||!n.length)return t;const s=[];let a=n.shift(),r=Wi.test(a);for(;;){if(r){const o=parseFloat(a);isNaN(o)?s.push(a):s.push(Math.ceil(o*e*i)/i)}else s.push(a);if(a=n.shift(),a===void 0)return s.join("");r=!r}}function Qi(t,e="defs"){let i="";const n=t.indexOf("<"+e);for(;n>=0;){const s=t.indexOf(">",n),a=t.indexOf("</"+e);if(s===-1||a===-1)break;const r=t.indexOf(">",a);if(r===-1)break;i+=t.slice(s+1,a).trim(),t=t.slice(0,n).trim()+t.slice(r+1)}return{defs:i,content:t}}function Yi(t,e){return t?"<defs>"+t+"</defs>"+e:e}function Zi(t,e,i){const n=Qi(t);return Yi(n.defs,e+n.content+i)}const Xi=t=>t==="unset"||t==="undefined"||t==="none";function qt(t,e){const i={...X,...t},n={...Et,...e},s={left:i.left,top:i.top,width:i.width,height:i.height};let a=i.body;[i,n].forEach(C=>{const $=[],F=C.hFlip,T=C.vFlip;let S=C.rotate;F?T?S+=2:($.push("translate("+(s.width+s.left).toString()+" "+(0-s.top).toString()+")"),$.push("scale(-1 1)"),s.top=s.left=0):T&&($.push("translate("+(0-s.left).toString()+" "+(s.height+s.top).toString()+")"),$.push("scale(1 -1)"),s.top=s.left=0);let k;switch(S<0&&(S-=Math.floor(S/4)*4),S=S%4,S){case 1:k=s.height/2+s.top,$.unshift("rotate(90 "+k.toString()+" "+k.toString()+")");break;case 2:$.unshift("rotate(180 "+(s.width/2+s.left).toString()+" "+(s.height/2+s.top).toString()+")");break;case 3:k=s.width/2+s.left,$.unshift("rotate(-90 "+k.toString()+" "+k.toString()+")");break}S%2===1&&(s.left!==s.top&&(k=s.left,s.left=s.top,s.top=k),s.width!==s.height&&(k=s.width,s.width=s.height,s.height=k)),$.length&&(a=Zi(a,'<g transform="'+$.join(" ")+'">',"</g>"))});const r=n.width,o=n.height,l=s.width,c=s.height;let u,p;r===null?(p=o===null?"1em":o==="auto"?c:o,u=Te(p,l/c)):(u=r==="auto"?l:r,p=o===null?Te(u,c/l):o==="auto"?c:o);const y={},x=(C,$)=>{Xi($)||(y[C]=$.toString())};x("width",u),x("height",p);const f=[s.left,s.top,l,c];return y.viewBox=f.join(" "),{attributes:y,viewBox:f,body:a}}function qe(t,e){let i=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const n in e)i+=" "+n+'="'+e[n]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+t+"</svg>"}function es(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function ts(t){return"data:image/svg+xml,"+es(t)}function zt(t){return'url("'+ts(t)+'")'}const is=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let ue=is();function ss(t){ue=t}function ns(){return ue}function as(t,e){const i=me(t);if(!i)return 0;let n;if(!i.maxURL)n=0;else{let s=0;i.resources.forEach(r=>{s=Math.max(s,r.length)});const a=e+".json?icons=";n=i.maxURL-s-i.path.length-a.length}return n}function rs(t){return t===404}const os=(t,e,i)=>{const n=[],s=as(t,e),a="icons";let r={type:a,provider:t,prefix:e,icons:[]},o=0;return i.forEach((l,c)=>{o+=l.length+1,o>=s&&c>0&&(n.push(r),r={type:a,provider:t,prefix:e,icons:[]},o=l.length),r.icons.push(l)}),n.push(r),n};function ls(t){if(typeof t=="string"){const e=me(t);if(e)return e.path}return"/"}const cs=(t,e,i)=>{if(!ue){i("abort",424);return}let n=ls(e.provider);switch(e.type){case"icons":{const a=e.prefix,r=e.icons.join(","),o=new URLSearchParams({icons:r});n+=a+".json?"+o.toString();break}case"custom":{const a=e.uri;n+=a.slice(0,1)==="/"?a.slice(1):a;break}default:i("abort",400);return}let s=503;ue(t+n).then(a=>{const r=a.status;if(r!==200){setTimeout(()=>{i(rs(r)?"abort":"next",r)});return}return s=501,a.json()}).then(a=>{if(typeof a!="object"||a===null){setTimeout(()=>{a===404?i("abort",a):i("next",s)});return}setTimeout(()=>{i("success",a)})}).catch(()=>{i("next",s)})},ds={prepare:os,send:cs};function us(t,e,i){A(i||"",e).loadIcons=t}function ps(t,e,i){A(i||"",e).loadIcon=t}const we="data-style";let Ht="";function hs(t){Ht=t}function dt(t,e){let i=Array.from(t.childNodes).find(n=>n.hasAttribute&&n.hasAttribute(we));i||(i=document.createElement("style"),i.setAttribute(we,we),t.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Ht}function Jt(){nt("",ds),Nt(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const i=t.IconifyPreload,n="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(s=>{try{(typeof s!="object"||s===null||s instanceof Array||typeof s.icons!="object"||typeof s.prefix!="string"||!st(s))&&console.error(n)}catch{console.error(n)}})}if(t.IconifyProviders!==void 0){const i=t.IconifyProviders;if(typeof i=="object"&&i!==null)for(const n in i){const s="IconifyProviders["+n+"] is invalid.";try{const a=i[n];if(typeof a!="object"||!a||a.resources===void 0)continue;at(n,a)||console.error(s)}catch{console.error(s)}}}}return{iconLoaded:Di,getIcon:Ii,listIcons:Pi,addIcon:Mt,addCollection:st,calculateSize:Te,buildIcon:qt,iconToHTML:qe,svgToURL:zt,loadIcons:Fe,loadIcon:Bi,addAPIProvider:at,setCustomIconLoader:ps,setCustomIconsLoader:us,appendCustomStyle:hs,_api:{getAPIConfig:me,setAPIModule:nt,sendAPIQuery:Ut,setFetch:ss,getFetch:ns,listAPIProviders:Li}}}const Ce={"background-color":"currentColor"},Bt={"background-color":"transparent"},ut={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},pt={"-webkit-mask":Ce,mask:Ce,background:Bt};for(const t in pt){const e=pt[t];for(const i in ut)e[t+"-"+i]=ut[i]}function ht(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function gs(t,e,i){const n=document.createElement("span");let s=t.body;s.indexOf("<a")!==-1&&(s+="<!-- "+Date.now()+" -->");const a=t.attributes,r=qe(s,{...a,width:e.width+"",height:e.height+""}),o=zt(r),l=n.style,c={"--svg":o,width:ht(a.width),height:ht(a.height),...i?Ce:Bt};for(const u in c)l.setProperty(u,c[u]);return n}let K;function fs(){try{K=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{K=null}}function ms(t){return K===void 0&&fs(),K?K.createHTML(t):t}function bs(t){const e=document.createElement("span"),i=t.attributes;let n="";i.width||(n="width: inherit;"),i.height||(n+="height: inherit;"),n&&(i.style=n);const s=qe(t.body,i);return e.innerHTML=ms(s),e.firstChild}function Ae(t){return Array.from(t.childNodes).find(e=>{const i=e.tagName&&e.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function gt(t,e){const i=e.icon.data,n=e.customisations,s=qt(i,n);n.preserveAspectRatio&&(s.attributes.preserveAspectRatio=n.preserveAspectRatio);const a=e.renderedMode;let r;a==="svg"?r=bs(s):r=gs(s,{...X,...i},a==="mask");const o=Ae(t);o?r.tagName==="SPAN"&&o.tagName===r.tagName?o.setAttribute("style",r.getAttribute("style")):t.replaceChild(r,o):t.appendChild(r)}function ft(t,e,i){const n=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:n}}function vs(t="iconify-icon"){let e,i;try{e=window.customElements,i=window.HTMLElement}catch{return}if(!e||!i)return;const n=e.get(t);if(n)return n;const s=["icon","mode","inline","noobserver","width","height","rotate","flip"],a=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const o=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");dt(o,l),this._state=ft({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return s.slice(0)}attributeChangedCallback(o){switch(o){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,dt(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const o=this.getAttribute("icon");if(o&&o.slice(0,1)==="{")try{return JSON.parse(o)}catch{}return o}set icon(o){typeof o=="object"&&(o=JSON.stringify(o)),this.setAttribute("icon",o)}get inline(){return this.hasAttribute("inline")}set inline(o){o?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(o){o?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const o=this._state;if(o.rendered){const l=this._shadowRoot;if(o.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}gt(l,o)}}get status(){const o=this._state;return o.rendered?"rendered":o.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const o=this._state,l=this.getAttribute("icon");if(l!==o.icon.value){this._iconChanged(l);return}if(!o.rendered||!this._visible)return;const c=this.getAttribute("mode"),u=tt(this);(o.attrMode!==c||ki(o.customisations,u)||!Ae(this._shadowRoot))&&this._renderIcon(o.icon,u,c)}_iconChanged(o){const l=Ki(o,(c,u,p)=>{const y=this._state;if(y.rendered||this.getAttribute("icon")!==c)return;const x={value:c,name:u,data:p};x.data?this._gotIconData(x):y.icon=x});l.data?this._gotIconData(l):this._state=ft(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const o=Ae(this._shadowRoot);o&&this._shadowRoot.removeChild(o);return}this._queueCheck()}_gotIconData(o){this._checkQueued=!1,this._renderIcon(o,tt(this),this.getAttribute("mode"))}_renderIcon(o,l,c){const u=Vi(o.data.body,c),p=this._state.inline;gt(this._shadowRoot,this._state={rendered:!0,icon:o,inline:p,customisations:l,attrMode:c,renderedMode:u})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(o=>{const l=o.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};s.forEach(o=>{o in a.prototype||Object.defineProperty(a.prototype,o,{get:function(){return this.getAttribute(o)},set:function(l){l!==null?this.setAttribute(o,l):this.removeAttribute(o)}})});const r=Jt();for(const o in r)a[o]=a.prototype[o]=r[o];return e.define(t,a),a}const ys=vs()||Jt(),{iconLoaded:Gs,getIcon:Ws,listIcons:Qs,addIcon:Ys,addCollection:Zs,calculateSize:Xs,buildIcon:en,iconToHTML:tn,svgToURL:sn,loadIcons:nn,loadIcon:an,setCustomIconLoader:rn,setCustomIconsLoader:on,addAPIProvider:ln,_api:cn}=ys;class ze extends Error{constructor(e,i){super(i),this.status=e,this.name="ApiRequestError"}}async function g(t,e){const i=await fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!i.ok){const n=await i.json().catch(()=>({error:i.statusText}));throw new ze(i.status,n.error||i.statusText)}return i.status===204?void 0:i.json()}function Ee(t,e,i=!1){if(e==="telegram"){const n=String(t.get("bot_token")??"");return{type:"telegram",name:t.get("name"),bot_token:i&&!n?void 0:n,chat_id:t.get("chat_id"),default:t.get("default")==="on"}}if(e==="smtp"){const n=String(t.get("username")??""),s=String(t.get("password")??"");return{type:"smtp",name:t.get("name"),host:t.get("host"),port:Number(t.get("port")),security:t.get("security"),username:n||void 0,password:s||void 0,from:t.get("from"),to:t.get("to"),default:t.get("default")==="on"}}return{type:"webhook",name:t.get("name"),url:t.get("url"),headers:i?void 0:{},default:t.get("default")==="on"}}function Pe(t,e=[],i=!0,n=String(t.get("kind")??"http")){const s=String(t.get("url")),a=n==="http"?s:`${n}://${s.replace(/^[a-z][a-z0-9+.-]*:\/\//i,"")}`;return{name:String(t.get("name")),kind:n,url:a,method:String(t.get("method")??"GET"),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:e,use_default_channels:i}}var xs=Object.defineProperty,ws=Object.getOwnPropertyDescriptor,U=(t,e,i,n)=>{for(var s=n>1?void 0:n?ws(e,i):e,a=t.length-1,r;a>=0;a--)(r=t[a])&&(s=(n?r(e,i,s):r(s))||s);return n&&s&&xs(e,i,s),s};let P=class extends M{constructor(){super(...arguments),this.channelKind="webhook",this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await g("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){if(t.preventDefault(),!window.confirm("Create a new single-Node Cluster?"))return;const e=new FormData(t.currentTarget),i=String(e.get("admin_username")??"").trim(),n=String(e.get("admin_password")??"");await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName(),admin_username:i,admin_password:n},{username:i,password:n})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,i=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(i.get("join_link")??"").trim()})}async choose(t,e,i){this.saving=!0,this.error="";try{await g(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster(i)}catch(n){this.fail(n),this.saving=!1}}async waitForCluster(t){for(let e=0;e<120;e+=1){const{promise:i,resolve:n}=Promise.withResolvers();window.setTimeout(n,250),await i;try{t&&await g("/api/v1/auth/login",{method:"POST",body:JSON.stringify(t)});const s=await g("/api/v1/setup");if(s.cluster_ready){this.changed(s);return}}catch(s){if(!t&&s instanceof ze&&s.status===401){window.location.assign("/");return}}}throw new Error("Cluster setup did not finish within 30 seconds")}async createChannel(t){t.preventDefault();const e=new FormData(t.currentTarget),i=Ee(e,this.channelKind);await this.createResource("/api/v1/channels",i)}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),i=Pe(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",i)}async createResource(t,e){this.saving=!0;try{await g(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(i){this.fail(i),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await g("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return d`<section class="flow" aria-label="UpGrid setup">
      ${this.error?d`<div class="notice" role="alert">${this.error}</div>`:h}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return d`
      <span class="eyebrow">First-run setup</span><h1>Choose your Cluster</h1>
      <p class="lead">Review this Node’s name, then create a new Cluster or use an invitation to join one.</p>
      <div class="cluster-panel">
        <div class="cluster-identity">
          <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} required /></label>
        </div>
        <form class="cluster-create" @submit=${this.createCluster}>
          <div class="cluster-copy"><h2>Start a new Cluster</h2><p>Create its first replicated administrator identity.</p></div>
          <div class="cluster-create-fields">
            <label>Administrator username<input name="admin_username" autocomplete="username" value="admin" required /></label>
            <label>Administrator password<input name="admin_password" type="password" minlength="12" autocomplete="new-password" required /></label>
          </div>
          <button type="submit" ?disabled=${this.saving}>${this.saving?"Setting up…":"Create new Cluster"}</button>
        </form>
        <div class="cluster-divider"><span>Or</span></div>
        <form class="cluster-join" @submit=${this.joinCluster}>
          <div class="cluster-copy"><h2>Join an existing Cluster</h2><p>Paste an <code>up://</code> Join Token from a current member.</p></div>
          <div class="cluster-join-fields">
            <label>Join Token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
            <button class="secondary" type="submit" ?disabled=${this.saving}>Join Cluster</button>
          </div>
        </form>
      </div>`}renderChannel(){return d`
      <span class="eyebrow">Optional · Step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions to Telegram or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createChannel}>
        <label>Type<select name="type" @change=${t=>this.channelKind=t.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
        <label>Name<input name="name" placeholder="On-call" required /></label>
        ${this.channelKind==="webhook"?d`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:d`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
        <label><span><input name="default" type="checkbox" checked /> Default channel</span></label>
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and continue</button></div>
      </form></div>`}renderTarget(){return d`
      <span class="eyebrow">Optional · Step 3 of 3</span><h1>Monitor your first Target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createTarget}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
        <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label></div>
        ${this.channels.length?d`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>d`<label><span><input name="channel_id" type="checkbox" value=${t.id} /> ${t.name}</span></label>`)}</fieldset>`:h}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`}};P.styles=je`
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    .flow { width: min(760px, 100%); margin: 0 auto; }
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .lead { margin: 0 0 16px; color: var(--muted); font-size: 15px; }
    .panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    .cluster-panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .cluster-identity, .cluster-create, .cluster-join { padding: 15px 18px; }
    .cluster-identity { border-bottom: 1px solid var(--line); }
    .cluster-create { display: grid; gap: 14px; }
    .cluster-create-fields { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .cluster-create button { justify-self: end; }
    .cluster-copy h2 { margin: 0; font-size: 17px; }
    .cluster-copy p { margin: 2px 0 0; color: var(--muted); }
    .cluster-divider { display: flex; align-items: center; gap: 12px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .12em; }
    .cluster-divider::before, .cluster-divider::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .cluster-join { display: grid; gap: 10px; }
    .cluster-join-fields { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; }
    .cluster-join-fields label { min-width: 0; }
    .cluster-join-fields button { height: 44px; white-space: nowrap; }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: border-color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { display: inline-flex; min-height: 44px; align-items: center; justify-content: center; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .65; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 620px) { .row, .cluster-create-fields, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-create button, .cluster-join button { justify-self: end; } }
    @media (max-height: 650px) and (min-width: 621px) {
      h1 { margin: 2px 0 4px; font-size: 30px; }
      .lead { margin-bottom: 8px; font-size: 13px; }
      .cluster-identity, .cluster-create, .cluster-join { padding: 8px 14px; }
      .cluster-create { grid-template-columns: minmax(0, 1fr) auto; gap: 8px; }
      .cluster-create .cluster-copy { grid-column: 1 / -1; }
      .cluster-create button { align-self: end; }
      .cluster-copy p { display: none; }
      .cluster-join { grid-template-columns: auto minmax(0, 1fr); align-items: end; }
      input, button { min-height: 38px; }
      .cluster-join-fields button { height: 44px; }
    }
  `;U([_t({attribute:!1})],P.prototype,"setup",2);U([m()],P.prototype,"channelKind",2);U([m()],P.prototype,"channels",2);U([m()],P.prototype,"saving",2);U([m()],P.prototype,"error",2);P=U([kt("upgrid-setup")],P);const $s={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},ks={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},_s={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var Ss=Object.defineProperty,v=(t,e,i,n)=>{for(var s=void 0,a=t.length-1,r;a>=0;a--)(r=t[a])&&(s=r(e,i,s)||s);return s&&Ss(e,i,s),s};const ae=["system","dark","bright"],mt={system:ks,dark:$s,bright:_s},He={overview:"/",alerts:"/alerts",cluster:"/cluster"};function Ts(t,e){if(!e)return{tone:"pending",label:"connecting"};const i=t.filter(s=>!s.paused);if(!i.length)return{tone:"pending",label:"ready"};const n=i.filter(s=>s.availability==="down"||s.consecutive_failures>0).length;return n?n===i.length?{tone:"down",label:"down"}:{tone:"degraded",label:"partially down"}:{tone:"up",label:"up"}}function bt(){return Object.entries(He).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function Cs(){const t=localStorage.getItem("upgrid-theme");return ae.includes(t)?t:"system"}class b extends M{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.transitions=[],this.secrets=[],this.joinTokens=[],this.identities=[],this.apiTokens=[],this.authReady=!1,this.newApiToken="",this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.channelTestMessage="",this.testingChannel=!1,this.joinCommand="",this.alertSearch="",this.alertDeliveryFilter="all",this.alertKindFilter="all",this.alertAcknowledgedFilter="all",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=bt(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!1,this.theme=Cs(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=bt()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await g("/api/v1/setup");e.cluster_ready&&(this.session=await g("/api/v1/auth/session")),await this.activate(e)}catch(e){(!(e instanceof ze)||e.status!==401)&&(this.error=e instanceof Error?e.message:String(e))}this.authReady=!0}async activate(e){if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}async login(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0,this.error="";try{this.session=await g("/api/v1/auth/login",{method:"POST",body:JSON.stringify({username:String(i.get("username")??""),password:String(i.get("password")??"")})}),await this.activate(await g("/api/v1/setup"))}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async logout(){await g("/api/v1/auth/logout",{method:"POST"}),this.events?.close(),this.session=void 0,this.live=!1,this.setupMode=!1,window.history.replaceState(null,"","/")}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=ae[(ae.indexOf(this.theme)+1)%ae.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.channels,this.alerts,this.transitions,this.secrets,this.cluster,this.joinTokens,this.identities,this.apiTokens]=await Promise.all([g("/api/v1/targets"),g("/api/v1/channels"),g("/api/v1/alerts"),g("/api/v1/transitions"),g("/api/v1/secrets"),g("/api/v1/cluster"),g("/api/v1/join-tokens"),g("/api/v1/identities"),g("/api/v1/api-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),n=i?.querySelector("form");n&&(this.detailInitialState=this.detailFormState(n)),i?.showModal()})}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,i){e.preventDefault(),this.activeSection=i,window.history.pushState(null,"",He[i])}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const i=e.currentTarget,n=i.form?.elements.namedItem("max_redirects");n&&(n.disabled=!i.checked),i.form&&this.compareDetailForm(i.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}}v([m()],b.prototype,"targets");v([m()],b.prototype,"channels");v([m()],b.prototype,"alerts");v([m()],b.prototype,"transitions");v([m()],b.prototype,"secrets");v([m()],b.prototype,"cluster");v([m()],b.prototype,"joinTokens");v([m()],b.prototype,"identities");v([m()],b.prototype,"apiTokens");v([m()],b.prototype,"session");v([m()],b.prototype,"authReady");v([m()],b.prototype,"newApiToken");v([m()],b.prototype,"error");v([m()],b.prototype,"live");v([m()],b.prototype,"saving");v([m()],b.prototype,"selected");v([m()],b.prototype,"channelKind");v([m()],b.prototype,"editingChannel");v([m()],b.prototype,"channelTestMessage");v([m()],b.prototype,"testingChannel");v([m()],b.prototype,"joinCommand");v([m()],b.prototype,"alertSearch");v([m()],b.prototype,"alertDeliveryFilter");v([m()],b.prototype,"alertKindFilter");v([m()],b.prototype,"alertAcknowledgedFilter");v([m()],b.prototype,"search");v([m()],b.prototype,"statusFilter");v([m()],b.prototype,"sort");v([m()],b.prototype,"selectedIds");v([m()],b.prototype,"activeSection");v([m()],b.prototype,"copied");v([m()],b.prototype,"setupMode");v([m()],b.prototype,"setup");v([m()],b.prototype,"warningDismissed");v([m()],b.prototype,"unlimitedUses");v([m()],b.prototype,"theme");v([m()],b.prototype,"detailDirty");class As extends b{async createTarget(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i),s=Pe(n,n.getAll("channel_id").map(String),n.get("use_default_channels")==="on");this.saving=!0;try{await g("/api/v1/targets",{method:"POST",body:JSON.stringify(s)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=new FormData(e.currentTarget);let n=`/api/v1/nodes/${this.selected.id}`,s={name:String(i.get("name"))};if(this.selected.kind==="http"){const a=i.get("follow_redirects")==="on";n=`/api/v1/targets/${this.selected.id}`,s={name:String(i.get("name")),kind:"http",url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:String(i.get("statuses")).split(",").map(r=>{const[o,l]=r.trim().split("-").map(Number);return{start:o,end:l||o}}),follow_redirects:a,max_redirects:a?Number(i.get("max_redirects")):0,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([r,o])=>[r,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(i.get("body_contains"))||null,skip_tls_verification:i.get("skip_tls_verification")==="on",notification_channel_ids:i.getAll("channel_id").map(String),use_default_channels:i.get("use_default_channels")==="on"}}this.selected.kind!=="http"&&this.selected.kind!=="node"&&(n=`/api/v1/targets/${this.selected.id}`,s=Pe(i,i.getAll("channel_id").map(String),i.get("use_default_channels")==="on",this.selected.kind)),this.saving=!0;try{await g(n,{method:"PUT",body:JSON.stringify(s)}),this.closeDetailDialog(),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await g(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await g(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i);this.saving=!0;try{await g("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:n.get("name"),value:n.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i),s=this.editingChannel,a=Ee(n,this.channelKind,s!==void 0);this.saving=!0;try{await g(s?`/api/v1/channels/${s.id}`:"/api/v1/channels",{method:s?"PUT":"POST",body:JSON.stringify(a)}),await this.refresh(),i.reset(),this.editingChannel=void 0,this.channelKind="webhook",this.channelTestMessage="",this.closeDialog("channel-dialog")}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}openChannelDialog(e){this.editingChannel=e,this.channelKind=e?.kind??"webhook",this.channelTestMessage="",this.showDialog("channel-dialog")}async setChannelDefault(e,i){try{await g(`/api/v1/channels/${e.id}/default`,{method:"PUT",body:JSON.stringify({default:i})}),await this.refresh()}catch(n){this.error=n instanceof Error?n.message:String(n)}}async testChannel(e){const i=e.currentTarget.form;if(!(!i||![...i.querySelectorAll("[data-test-required]")].every(s=>s.reportValidity()))){this.testingChannel=!0,this.channelTestMessage="";try{const s=Ee(new FormData(i),this.channelKind);await g("/api/v1/channels/test",{method:"POST",body:JSON.stringify(s)}),this.channelTestMessage="Test sent"}catch(s){const a=s instanceof Error?s.message:String(s);this.channelTestMessage=`Test failed: ${a}`}finally{this.testingChannel=!1}}}openTokenDialog(){this.unlimitedUses=!1,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0;try{const n=await g("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(i.get("expiration_days"))*86400,max_uses:this.unlimitedUses?null:Number(i.get("max_uses"))})});this.joinCommand=`upgrid --join '${n.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async createIdentity(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i);await this.saveResource(async()=>{await g("/api/v1/identities",{method:"POST",body:JSON.stringify({username:String(n.get("username")??""),password:String(n.get("password")??"")})}),i.reset()})}async updateIdentity(e,i){i.preventDefault();const n=new FormData(i.currentTarget),s=String(n.get("password")??"");await this.saveResource(async()=>{await g(`/api/v1/identities/${e.id}`,{method:"PUT",body:JSON.stringify({username:String(n.get("username")??""),password:s||null})}),e.id===this.session?.identity_id&&s&&await this.logout()})}async deleteIdentity(e){window.confirm(`Delete identity ${e.username}? Its API Tokens will also be revoked.`)&&await this.saveResource(()=>g(`/api/v1/identities/${e.id}`,{method:"DELETE"}))}async createApiToken(e){e.preventDefault();const i=e.currentTarget,n=new FormData(i);await this.saveResource(async()=>{const s=Number(n.get("expires_in_days")),a=await g("/api/v1/api-tokens",{method:"POST",body:JSON.stringify({name:String(n.get("name")??""),expires_in_seconds:s?s*86400:null})});this.newApiToken=a.value,i.reset()})}async revokeApiToken(e){window.confirm(`Revoke API Token ${e.name}?`)&&await this.saveResource(()=>g(`/api/v1/api-tokens/${e.id}`,{method:"DELETE"}))}async setNodeDrain(e,i){await this.saveResource(()=>g(`/api/v1/nodes/${e.id}/drain`,{method:"PUT",body:JSON.stringify({draining:i,force:!1})}))}async removeNode(e,i){const n=i?`Replace failed Node ${e.name}? Confirm that it is permanently stopped. Its assignments will be released immediately.`:`Remove drained Node ${e.name} from the Cluster?`;window.confirm(n)&&(await this.saveResource(()=>g(`/api/v1/nodes/${e.id}?force=${i}`,{method:"DELETE"})),i&&!this.error&&this.openTokenDialog())}async acknowledgeAlert(e){await this.updateAlert("acknowledge",e)}async retryAlert(e){await this.updateAlert("retry",e)}async updateAlert(e,i){await this.saveResource(()=>g(`/api/v1/alerts/${e}`,{method:"POST",body:JSON.stringify({target_id:i.target_id,channel_id:i.channel_id,scheduled_at_ms:i.scheduled_at_ms,kind:i.kind})}))}async saveResource(e){this.saving=!0,this.error="";try{await e(),this.session&&await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async setupChanged(e){const i=e.detail;if(this.setup=i,this.setupMode=i.setup,window.history.replaceState(null,"",i.path),i.setup){i.cluster_ready&&(this.session=await g("/api/v1/auth/session"),await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await g(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const i=Object.assign(document.createElement("textarea"),{value:this.joinCommand});i.style.cssText="position: fixed; opacity: 0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const n=new Set(this.selectedIds);i?n.add(e):n.delete(e),this.selectedIds=n}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>g(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>g(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,i,n){if(window.confirm(`Delete ${n}?`))try{await g(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}}}const Es={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5L2 22l1.5-5.5Zm-2 2l4 4"/>'};function Ps(t,e){const i=e.search.trim().toLocaleLowerCase();return(!i||`${t.target_name} ${t.channel_name}`.toLocaleLowerCase().includes(i))&&(e.delivery==="all"||t.delivery===e.delivery)&&(e.kind==="all"||t.kind===e.kind)&&(e.acknowledged==="all"||(e.acknowledged==="yes"?t.acknowledged_at_ms!==null:t.acknowledged_at_ms===null))}function Ds(t){return t.delivery==="pending"?t.next_attempt_at_ms===null?`${t.attempts} attempts`:`${t.attempts} attempts · next ${new Date(t.next_attempt_at_ms).toLocaleString()}`:t.delivery==="failed"?t.diagnostic??"Delivery failed":t.completed_at_ms===null?"Delivered":`Delivered ${new Date(t.completed_at_ms).toLocaleString()}`}function Is(t,e,i,n,s,a){const r=t.filter(o=>Ps(o,n));return d`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${a.create}>Add channel</button>
    </section>
    <section class="panel alert-history" aria-label="Alert history">
      <div class="panel-head"><h2>Notification deliveries</h2><span class="meta">${r.length} of ${t.length} alerts</span></div>
      <div class="alert-filters">
        <label>Search<input type="search" .value=${n.search} placeholder="Target or channel" @input=${o=>a.setSearch(o.target.value)} /></label>
        <label>Delivery<select .value=${n.delivery} @change=${o=>a.setDelivery(o.target.value)}><option value="all">All</option><option value="pending">Pending</option><option value="delivered">Delivered</option><option value="failed">Failed</option></select></label>
        <label>Transition<select .value=${n.kind} @change=${o=>a.setKind(o.target.value)}><option value="all">All</option><option value="down">Down</option><option value="recovered">Recovered</option></select></label>
        <label>Acknowledged<select .value=${n.acknowledged} @change=${o=>a.setAcknowledged(o.target.value)}><option value="all">All</option><option value="no">No</option><option value="yes">Yes</option></select></label>
      </div>
      ${r.length?r.map(o=>d`
                <div class="resource alert-resource">
                  <div class="alert-summary">
                    <div class="channel-title">
                      <strong>${o.target_name}</strong>
                      <span class=${`badge ${o.kind==="recovered"?"up":"down"}`}>${o.kind}</span>
                      <span class="badge">${o.delivery}</span>
                      ${o.acknowledged_at_ms===null?h:d`<span class="badge">acknowledged</span>`}
                    </div>
                    <code>${o.channel_name} · ${new Date(o.scheduled_at_ms).toLocaleString()}</code>
                    <span class="meta">${Ds(o)}</span>
                  </div>
                  <div class="alert-actions">
                    ${o.delivery==="failed"?d`<button class="button secondary" ?disabled=${s} @click=${()=>a.retry(o)}>Retry</button>`:h}
                    ${o.acknowledged_at_ms===null?d`<button class="button secondary" ?disabled=${s} @click=${()=>a.acknowledge(o)}>Acknowledge</button>`:h}
                  </div>
                </div>
              `):d`<div class="empty">No alerts match these filters.</div>`}
    </section>
    <div class="page-columns">
      <section class="panel" aria-label="Availability history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${e.length} events</span></div>
        ${e.length?e.map(o=>{const l=o.kind==="recovered"?"up":"down";return d`
                <div class="resource">
                  <div class="transition-main">
                    <span class=${`state ${l}`} aria-hidden="true"></span>
                    <div>
                      <strong>${o.target_name}</strong>
                      <code>${new Date(o.scheduled_at_ms).toLocaleString()}</code>
                    </div>
                  </div>
                  <span class=${`badge ${l}`}>${o.kind}</span>
                </div>
              `}):d`<div class="empty">No availability transitions.</div>`}
      </section>
      <section class="panel" aria-label="Notification channels">
        <div class="panel-head"><h2>Notification channels</h2><span class="meta">${i.length} configured</span></div>
        ${i.length?i.map(o=>d`
              <div class="resource channel-resource">
                <div class="channel-summary"><div class="channel-title"><strong>${o.name}</strong><span class="badge">${o.kind}</span></div><code>${o.destination}</code></div>
                <div class="channel-actions">
                  <label class="switch"><span>Default</span><input type="checkbox" role="switch" aria-label=${`Default channel ${o.name}`} .checked=${o.default} @change=${l=>a.setDefault(o,l.target.checked)} /></label>
                  <button class="button secondary icon-button" aria-label=${`Edit channel ${o.name}`} title=${`Edit ${o.name}`} @click=${()=>a.edit(o)}><iconify-icon .icon=${Es} aria-hidden="true"></iconify-icon></button>
                  <button class="button danger icon-button" aria-label=${`Delete channel ${o.name}`} title=${`Delete ${o.name}`} @click=${()=>a.remove(o)}><iconify-icon .icon=${le} aria-hidden="true"></iconify-icon></button>
                </div>
              </div>
            `):d`<div class="empty">No notification channels.</div>`}
      </section>
    </div>
  `}function Os(t,e,i){return d`
    <main class="shell setup-shell">
      <header>
        <div class="brand"><img src="/favicon.svg" alt="" /><div><strong>UpGrid</strong><span>Distributed service monitoring</span></div></div>
      </header>
      <section class="panel auth-panel" aria-labelledby="login-title">
        <form class="choice" @submit=${i.login}>
          <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated Operator Identity.</p></div>
          ${e?d`<div class="notice" role="alert">${e}</div>`:h}
          <label>Username<input name="username" autocomplete="username" required autofocus /></label>
          <label>Password<input name="password" type="password" autocomplete="current-password" required /></label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${t}>${t?"Signing in…":"Sign in"}</button></div>
        </form>
      </section>
    </main>`}function js(t,e,i,n,s,a){return d`
    <div class="page-columns access-panels">
      <section class="panel" aria-label="Operator Identities">
        <div class="panel-head"><h2>Operator Identities</h2><span class="meta">${t.length} administrators</span></div>
        ${t.map(r=>d`
            <div class="resource access-resource">
              <form class="access-form" @submit=${o=>a.updateIdentity(r,o)}>
                <label>Username<input name="username" .value=${r.username} required /></label>
                <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
                <button class="button secondary" type="submit" ?disabled=${s}>Save</button>
              </form>
              <button class="button danger" type="button" ?disabled=${r.id===i||s} @click=${()=>a.deleteIdentity(r)}>Delete</button>
            </div>`)}
        <form class="choice compact-form" @submit=${a.createIdentity}>
          <h3>Add administrator</h3>
          <label>Username<input name="username" required /></label>
          <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
          <button class="button" type="submit" ?disabled=${s}>Add identity</button>
        </form>
      </section>
      <section class="panel" aria-label="API Tokens">
        <div class="panel-head"><h2>API Tokens</h2><span class="meta">${e.length} active</span></div>
        ${n?d`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${n}</code><button class="button secondary" @click=${a.dismissToken}>Dismiss</button></div>`:h}
        ${e.length?e.map(r=>d`<div class="resource"><div><strong>${r.name}</strong><code>${r.expires_at_ms?`Expires ${new Date(r.expires_at_ms).toLocaleString()}`:"Never expires"}</code></div><button class="button danger" @click=${()=>a.revokeApiToken(r)}>Revoke</button></div>`):d`<div class="empty">No API Tokens.</div>`}
        <form class="choice compact-form" @submit=${a.createApiToken}>
          <h3>Create API Token</h3>
          <label>Name<input name="name" placeholder="Automation" required /></label>
          <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
          <button class="button" type="submit" ?disabled=${s}>Create API Token</button>
        </form>
      </section>
    </div>`}const Ns={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4m0-4h.01"/></g>'},Ms=je`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { display: flex; align-items: center; gap: 3px; }
  .help-tooltip-wrap { position: relative; display: inline-flex; align-items: center; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: help; transition: background-color 160ms ease, color 160ms ease; }
  .help-tooltip-trigger:hover { background: var(--panel-2); color: var(--text); }
  .help-tooltip-trigger iconify-icon { width: 16px; height: 16px; font-size: 16px; }
  .help-tooltip { position: absolute; top: calc(100% + 6px); left: -60px; z-index: 10; width: 280px; max-width: calc(100vw - 64px); border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--text); box-shadow: 0 10px 30px var(--dialog-shadow); padding: 9px 10px; font-size: 12px; font-weight: 400; line-height: 1.45; opacity: 0; visibility: hidden; transform: translateY(-3px); pointer-events: none; transition: opacity 140ms ease, transform 140ms ease, visibility 140ms; }
  .help-tooltip-wrap:hover .help-tooltip, .help-tooltip-wrap:focus-within .help-tooltip { opacity: 1; visibility: visible; transform: translateY(0); }
`;function pe(t,e,i){return d`
    <span class="help-tooltip-wrap">
      <button class="help-tooltip-trigger" type="button" aria-label=${e} aria-describedby=${t}>
        <iconify-icon .icon=${Ns} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip" id=${t} role="tooltip">${i}</span>
    </span>
  `}function Rs(t,e){return t==="webhook"?d`<label
      >Webhook URL<input
        name="url"
        type="url"
        placeholder="https://hooks.example.com/upgrid"
        .value=${e?.destination??""}
        data-test-required
        required
    /></label>`:t==="telegram"?d`
      <label
        ><span class="title-with-help"
          >Bot token
          ${pe("telegram-token-help","About Telegram bot token storage",e?"Leave this blank to keep the automatically managed Secret, or enter a replacement token.":"Creating the Channel encrypts this token as an automatically managed Secret. Test sends use the entered value without storing it.")}</span
        ><input
          name="bot_token"
          type="password"
          autocomplete="off"
          placeholder=${e?"Leave blank to keep current token":""}
          ?required=${e===void 0}
      /></label>
      <label
        >Chat ID<input name="chat_id" .value=${e?.destination??""} data-test-required required
      /></label>
    `:d`
    <label
      >SMTP host<input name="host" placeholder="smtp.example.com" .value=${e?.destination??""} required
    /></label>
    <div class="row">
      <label
        >Port<input
          name="port"
          type="number"
          min="1"
          max="65535"
          .value=${String(e?.port??587)}
          required
      /></label>
      <label
        >Security<select name="security" .value=${e?.security??"start_tls"}>
          <option value="start_tls">STARTTLS</option>
          <option value="tls">Implicit TLS</option>
          <option value="none">Plaintext</option>
        </select></label
      >
    </div>
    <label
      >Username<input name="username" autocomplete="username" .value=${e?.username??""}
    /></label>
    <div class="form-field">
      <div class="title-with-help">
        <label for="smtp-password">Password</label>
        ${pe("smtp-password-help","About SMTP password storage",e?"Leave this blank to keep the automatically managed Secret. Clear the username to disable authentication.":"Enter a username and password together to enable authentication. The password is encrypted as an automatically managed Secret.")}
      </div>
      <input
        id="smtp-password"
        name="password"
        type="password"
        autocomplete="off"
        placeholder=${e?"Leave blank to keep current password":"Optional"}
      />
    </div>
    <label
      >From<input name="from" placeholder="UpGrid <upgrid@example.com>" .value=${e?.from??""} required
    /></label>
    <label
      >Recipient<input name="to" placeholder="on-call@example.com" .value=${e?.to??""} required
    /></label>
  `}const Ls={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5c.08-1.25-.27-2.48-1-3.5c.28-1.15.28-2.35 0-3.5c0 0-1 0-3 1.5c-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5c-.39.49-.68 1.05-.85 1.65c-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></g>'},Us={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M21.54 15H17a2 2 0 0 0-2 2v4.54M7 3.34V5a3 3 0 0 0 3 3v0a2 2 0 0 1 2 2v0c0 1.1.9 2 2 2v0a2 2 0 0 0 2-2v0c0-1.1.9-2 2-2h3.17M11 21.95V18a2 2 0 0 0-2-2v0a2 2 0 0 1-2-2v-1a2 2 0 0 0-2-2H2.05"/><circle cx="12" cy="12" r="10"/></g>'};function $e(){return d`
    <footer aria-label="Project information">
      <div class="footer-links">
        <a href="https://miao.dev">A Project by Pop</a>
        <span aria-hidden="true">|</span>
        <a href="https://github.com/George-Miao/UpGrid">
          <iconify-icon .icon=${Ls} aria-hidden="true"></iconify-icon>GitHub
        </a>
        <span aria-hidden="true">|</span>
        <a href="https://upgrid.rs">
          <iconify-icon .icon=${Us} aria-hidden="true"></iconify-icon>upgrid.rs
        </a>
      </div>
      <div class="footer-powered">
        Proudly powered by <a href="https://compio.rs/">Compio</a> and
        <a href="https://github.com/databendlabs/openraft">OpenRaft</a>
      </div>
    </footer>
  `}function Kt(t,e=[],i=!0){return d`
    <fieldset class="channel-fields">
      <legend>Notification channels</legend>
      <label class="switch">
        <span>Use default channels</span>
        <input
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${i}
          @change=${s=>{const a=s.currentTarget;a.closest("fieldset")?.querySelectorAll('input[data-default="true"]').forEach(o=>{o.disabled=a.checked,o.checked=a.checked||o.dataset.explicit==="true"}),a.form?.dispatchEvent(new Event("input",{bubbles:!0}))}}
        />
      </label>
      <div class="channel-options">
        ${t.map(s=>{const a=e.includes(s.id),r=i&&s.default;return d`
            <label class="check">
              <input
                name="channel_id"
                type="checkbox"
                value=${s.id}
                data-default=${String(s.default)}
                data-explicit=${String(a)}
                .checked=${a||r}
                ?disabled=${r}
                @change=${o=>{const l=o.currentTarget;l.dataset.explicit=String(l.checked)}}
              />
              ${s.name} <span class="badge">${s.kind}</span>
            </label>
          `})}
      </div>
    </fieldset>`}const Vt={http:"https://example.com/health",tcp:"database.internal:5432",dns:"service.internal",icmp:"192.0.2.10",tls:"example.com:443"};function Gt(t,e){const i=t.elements.namedItem("url");i&&(i.placeholder=Vt[e],i.type=e==="http"?"url":"text");const n=t.querySelector("[data-http-options]");n&&(n.hidden=e!=="http");const s=t.elements.namedItem("method");s&&(s.disabled=e!=="http",s.disabled&&(s.value="GET"))}function Fs(t){const e=t.currentTarget;e.form&&Gt(e.form,e.value)}function qs(t){const e=t.currentTarget;queueMicrotask(()=>Gt(e,"http"))}function zs(t,e,i){return d`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${i.backdrop}>
      <div class="dialog-head"><div class="title-with-help"><h2 id="add-target-title">Add target</h2>${pe("target-secret-help","About Target Secrets","Advanced Target headers and request bodies can reference reusable Secrets through the HTTP API.")}</div><p>Start monitoring a service.</p></div>
      <form @submit=${i.create} @reset=${qs}>
        <label>Name<input name="name" placeholder="Production API" required autofocus /></label>
        <div class="row">
          <label>Type<select name="kind" @change=${Fs}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select></label>
          <label>URL / endpoint<input name="url" type="url" placeholder=${Vt.http} required /></label>
        </div>
        <label data-http-options>Method<input name="method" value="GET" required /></label>
        <div class="row">
          <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
        </div>
        <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
        ${Kt(t)}
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${i.close}>Cancel</button>
          <button class="button" type="submit" ?disabled=${e}>${e?"Creating…":"Create target"}</button>
        </div>
      </form>
    </dialog>`}function Hs(t,e,i,n,s,a){const r=t.kind==="node",o=t.kind==="http",l=t.accepted_statuses.map(f=>f.start===f.end?f.start:`${f.start}-${f.end}`).join(","),c=t.history.slice(0,30).reverse(),u=Math.max(1,...c.map(f=>f.latency_ms)),p=new Map(n.map(f=>[f.id,f.name])),y=f=>new Date(f).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),x=f=>f>=1e3?`${(f/1e3).toFixed(f>=1e4?0:1)} s`:`${Math.round(f)} ms`;return d`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${a.backdrop}>
      <div class="dialog-head">
        <h2 id="target-detail-title">${r?"Node details":"Target details"}</h2>
        <button class="button secondary icon-button dialog-close" type="button" aria-label=${`Close ${r?"Node":"Target"} details`} title="Close" @click=${a.close}><iconify-icon .icon=${Ct} aria-hidden="true"></iconify-icon></button>
      </div>
      <form @submit=${a.update} @input=${a.changed}>
        <label>Name<input name="name" .value=${t.name} required /></label>
        ${r?d`<label>RPC URL<input .value=${t.url} disabled /></label>`:d`
              <div class="row"><label>Type<input .value=${t.kind.toUpperCase()} disabled /></label><label>URL / endpoint<input name="url" .value=${t.url} required /></label></div>
              ${o?d`
                    <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${l} required /></label></div>
                    <label>Body must contain<input name="body_contains" .value=${t.body_contains??""} /></label>
                    <div class="row"><label class="check"><input name="follow_redirects" type="checkbox" .checked=${t.follow_redirects} @change=${a.redirects} />Follow redirects</label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
                    <label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${t.skip_tls_verification} />Skip TLS verification</label>
                  `:h}
              <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
              <label>Failures before Down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label>
              ${Kt(s,t.notification_channel_ids,t.use_default_channels)}
            `}
        <div class="dialog-actions">
          ${r?h:d`<div class="danger-actions">
            <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${a.delete}><iconify-icon .icon=${le} aria-hidden="true"></iconify-icon></button>
            <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>a.pause(!t.paused)}><iconify-icon .icon=${t.paused?Tt:St} aria-hidden="true"></iconify-icon></button>
          </div>`}
          <button class="button" type="submit" aria-busy=${e?"true":"false"} ?disabled=${e||!i}>Save changes</button>
        </div>
      </form>
      <section class="history">
        <div class="history-head"><h3>Evaluation history</h3>${c.length?d`<span class="meta">Latest ${c.length}</span>`:h}</div>
        ${c.length?d`
          <div class="chart-plot">
            <div class="chart-scale" aria-hidden="true"><span>${x(u)}</span><span>${x(u/2)}</span><span>0 ms</span></div>
            <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${x(u)}`}>
              ${c.map(f=>{const C=f.succeeded?"Passed":"Failed",$=r||!o?f.succeeded?"reachable":"unreachable":f.status_code===null?"network error":`HTTP ${f.status_code}`,F=p.get(f.executor_node_id)??`Node ${f.executor_node_id.slice(0,8)}`,T=`${C} at ${new Date(f.recorded_at_ms).toLocaleString()}: ${f.latency_ms} ms, ${$}. Executed by ${F}`;return d`<span class="history-bar ${f.succeeded?"up":"down"}" role="listitem" aria-label=${T} title=${T} style=${`height: ${Math.max(8,f.latency_ms/u*100)}%`}></span>`})}
            </div>
          </div>
          <div class="chart-axis"><span>${y(c[0].recorded_at_ms)}</span><span>${y(c[c.length-1].recorded_at_ms)}</span></div>
          <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
        `:d`<p class="meta">No evaluations recorded yet.</p>`}
      </section>
    </dialog>`}var Js=Object.getOwnPropertyDescriptor,Bs=(t,e,i,n)=>{for(var s=n>1?void 0:n?Js(e,i):e,a=t.length-1,r;a>=0;a--)(r=t[a])&&(s=r(s)||s);return s};let De=class extends As{render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,i=this.alerts.filter(r=>r.delivery==="pending").length,n=Ts(this.targets,this.live),s=["overview","alerts","cluster"],a=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name));return this.authReady&&!this.setupMode&&!this.session?d`${Os(this.saving,this.error,{login:r=>{this.login(r)}})}${$e()}`:this.setupMode&&this.setup?d`
        <main class="shell setup-shell">
          <header>
            <div class="brand">
              <img src="/favicon.svg" alt="" />
              <div><div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"up":""}"></i>${this.live?"ready":"connecting"}</div></div><span>Distributed service monitoring</span></div>
            </div>
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${mt[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error?d`<div class="notice" role="alert">${this.error}</div>`:h}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>${$e()}`:d`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${n.tone}"></i>${n.label}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${s.map(r=>d`<a class=${this.activeSection===r?"active":""} href=${He[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${mt[this.theme]} aria-hidden="true"></iconify-icon></button>
            <span class="meta">${this.session?.username}</span>
            <button class="button secondary" @click=${()=>{this.logout()}}>Sign out</button>
          </div>
        </header>
        ${this.error?d`<div class="notice" role="alert">${this.error}</div>`:h}
        ${this.setup?.warning&&!this.warningDismissed?d`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:h}
        ${this.activeSection==="overview"?this.renderOverview(a,t,e,i):this.activeSection==="alerts"?Is(this.alerts,this.transitions,this.channels,{search:this.alertSearch,delivery:this.alertDeliveryFilter,kind:this.alertKindFilter,acknowledged:this.alertAcknowledgedFilter},this.saving,{create:()=>this.openChannelDialog(),edit:r=>this.openChannelDialog(r),remove:r=>{this.deleteResource("channels",r.id,r.name)},setDefault:(r,o)=>{this.setChannelDefault(r,o)},acknowledge:r=>{this.acknowledgeAlert(r)},retry:r=>{this.retryAlert(r)},setSearch:r=>this.alertSearch=r,setDelivery:r=>this.alertDeliveryFilter=r,setKind:r=>this.alertKindFilter=r,setAcknowledged:r=>this.alertAcknowledgedFilter=r}):this.renderClusterPage()}
      </main>${$e()}
      ${zs(this.channels,this.saving,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeTargetDialog(),create:r=>{this.createTarget(r)}})}
      ${this.selected?Hs(this.selected,this.saving,this.detailDirty,this.cluster?.members??[],this.channels,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),redirects:r=>this.toggleMaxRedirects(r),delete:()=>{this.deleteTarget()},pause:r=>{this.setPaused(r)}}):h}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="secret-title">Add secret</h2><p>Create an encrypted, write-only value to reference from Target requests or webhook headers through the HTTP API.</p></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">${this.editingChannel?"Edit channel":"Add channel"}</h2><p>${this.editingChannel?"Update this destination without changing its Channel type.":"Send transitions through Telegram, SMTP, or a generic webhook."}</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" .value=${this.channelKind} ?disabled=${this.editingChannel!==void 0} @change=${r=>{this.channelKind=r.target.value,this.channelTestMessage=""}}><option value="webhook">Webhook</option><option value="telegram">Telegram</option><option value="smtp">SMTP email</option></select></label>
          <label>Name<input name="name" placeholder="On-call" .value=${this.editingChannel?.name??""} required /></label>
          ${Rs(this.channelKind,this.editingChannel)}
          <label class="switch"><span>Default channel</span><input name="default" type="checkbox" role="switch" .checked=${this.editingChannel?.default??!1} /></label>
          <div class="dialog-actions">${this.channelTestMessage?d`<span class="meta" role="status" style="margin-right:auto">${this.channelTestMessage}</span>`:h}<button class="button secondary" type="button" @click=${()=>{this.editingChannel=void 0,this.closeDialog("channel-dialog")}}>Cancel</button>${this.editingChannel?h:d`<button class="button secondary" type="button" aria-busy=${this.testingChannel} ?disabled=${this.testingChannel||this.saving} @click=${this.testChannel}>${this.testingChannel?"Sending…":"Send test"}</button>`}<button class="button" type="submit" ?disabled=${this.saving||this.testingChannel}>${this.editingChannel?"Save changes":"Create channel"}</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create Join Token</h2><p>Choose how many days the token remains valid and whether it can be reused.</p></div>
        <form @submit=${this.createJoinToken}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required /></label>
          <label class="switch"><span>Unlimited uses</span><input type="checkbox" role="switch" .checked=${this.unlimitedUses} @change=${r=>this.unlimitedUses=r.target.checked} /></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join Token Created</h2><p>This command contains Cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderOverview(t,e,i,n){const s=this.targets.filter(o=>this.selectedIds.has(o.id)),a=s.some(o=>!o.paused),r=s.some(o=>o.paused);return d`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="overview-top">
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${n}</strong></div>
          <div class="metric"><span>Up</span><strong>${e}</strong></div>
          <div class=${`metric down ${i?"active":""}`}><span>Down</span><strong>${i}</strong></div>
        </section>
        <section class="panel" aria-label="Secrets">
          <div class="panel-head"><div class="title-with-help"><h2>Secrets</h2>${pe("secrets-help","About reusable Secrets","Reusable Secrets are encrypted and write-only. Reference their IDs in Target headers or bodies and webhook headers through the HTTP API.")}</div><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length?this.secrets.map(o=>d`<div class="resource"><div><strong>${o.name}</strong><code>${o.id}</code></div><button class="button danger icon-button" aria-label=${`Delete secret ${o.name}`} title=${`Delete ${o.name}`} @click=${()=>this.deleteResource("secrets",o.id,o.name)}><iconify-icon .icon=${le} aria-hidden="true"></iconify-icon></button></div>`):d`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${o=>this.search=o.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${o=>this.statusFilter=o.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${o=>this.sort=o.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?d`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${Ct} aria-hidden="true"></iconify-icon></button>${a?d`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${St} aria-hidden="true"></iconify-icon></button>`:h}${r?d`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${Tt} aria-hidden="true"></iconify-icon></button>`:h}<button class="button danger icon-button" aria-label="Delete selected" title="Delete selected" @click=${this.bulkDelete}><iconify-icon .icon=${le} aria-hidden="true"></iconify-icon></button></div></div>`:h}
        ${t.length?t.map(o=>this.renderTarget(o)):d`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
    `}renderClusterMember(t){return d`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.raft_url} · ${t.active_assignments} active assignments</code>
        </div>
        <div class="actions">
          ${t.local?d`<span class="badge">This node</span>`:h}
          ${t.leader?d`<span class="badge">Leader</span>`:h}
          ${t.draining?d`<span class="badge">Draining</span>`:h}
          ${t.local?h:d`
                <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.setNodeDrain(t,!t.draining)}>${t.draining?"Cancel drain":"Drain"}</button>
                ${t.draining&&t.active_assignments===0?d`<button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!1)}>Remove</button>`:h}
                <button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!0)}>Replace failed</button>
              `}
        </div>
      </div>
    `}renderClusterPage(){return d`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><div><h2>Nodes</h2><p class="meta">Drain healthy Nodes before removal. Replace failed Nodes only after confirming the old process is permanently stopped.</p></div><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(t=>this.renderClusterMember(t))}
        ${this.cluster?.members.length?h:d`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(t=>d`
              <div class="resource">
                <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
              </div>
            `):d`<div class="empty">No Join Tokens.</div>`}
      </section>
      </div>
      ${js(this.identities,this.apiTokens,this.session?.identity_id,this.newApiToken,this.saving,{login:t=>{this.login(t)},logout:()=>{this.logout()},createIdentity:t=>{this.createIdentity(t)},updateIdentity:(t,e)=>{this.updateIdentity(t,e)},deleteIdentity:t=>{this.deleteIdentity(t)},createApiToken:t=>{this.createApiToken(t)},revokeApiToken:t=>{this.revokeApiToken(t)},dismissToken:()=>this.newApiToken=""})}
    `}renderTarget(t){const e=t.kind==="node",i=t.kind==="http",n=t.latest_evaluation,s=t.history.slice(0,16).reverse(),a=Math.max(1,...s.map(o=>o.latency_ms)),r=t.paused?"paused":t.availability==="down"?"down":t.consecutive_failures>0?"suspicious":t.availability;return d`
      <div class="target-wrap">
        ${e?d`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} disabled />`:d`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${o=>this.toggleSelected(t.id,o.target.checked)} />`}
        <button class=${`target ${e?"node-target":""}`} aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${r}" aria-label=${r}></i>
          <div>
            <div class="target-title"><h3>${t.name}</h3><span class="badge">${e?"Node":t.kind.toUpperCase()}</span></div>
            <div class="meta">${t.paused?"Paused · ":""}${i||e?`${t.method} · `:""}${t.url} · every ${t.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${s.length?d`<div class="mini-chart" aria-hidden="true">${s.map(o=>d`<i class="mini-bar ${o.succeeded?"up":"down"}" style=${`height: ${Math.max(12,o.latency_ms/a*100)}%`}></i>`)}</div>`:h}
            <div class="latency">
              <strong>${n?`${n.latency_ms} ms`:"—"}</strong>
              <span>${n?i?n.status_code??"network error":n.succeeded?"reachable":"unreachable":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};De.styles=je`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      --page-background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      --brand-shadow: #40d89035;
      --nav-bg: #0d1210aa;
      --active-bg: #202b27;
      --button-border: #3e765a;
      --button-bg: #1c4a35;
      --button-text: #e8fff2;
      --button-hover-border: #62b988;
      --panel-surface: #111715dc;
      --panel-shadow: #0002;
      --divider: #202925;
      --badge-border: #3c554a;
      --badge-text: #a7c3b7;
      --row-hover: #17201c;
      --notice-border: #7b3937;
      --notice-bg: #391b1a;
      --notice-text: #ffb3af;
      --bulk-bg: #16221d;
      --dialog-shadow: #000b;
      --backdrop: #040706cc;
      --input-bg: #0c110f;
      --focus: #4b936c;
      --danger-text: #ff9b97;
      --danger-border: #633b39;
      --warning-bg: #594315;
      --warning-text: #ffd778;
      --warning-border: #9c7625;
      --join-bg: #0b110e;
      display: flex; flex-direction: column;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { flex: 1 0 auto; width: 100%; max-width: 1200px; margin: auto; padding: 28px 24px 48px; }
    .setup-shell { display: grid; grid-template-rows: auto minmax(0, 1fr); padding-top: 20px; padding-bottom: 20px; }
    .setup-shell header { margin-bottom: 18px; } .setup-shell upgrid-setup { align-self: center; }
    header { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); align-items: center; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    header > .brand { justify-self: start; }
    header > nav { justify-self: center; }
    header > .actions { justify-self: end; }
    .brand-line { display: flex; align-items: center; gap: 12px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); transition: background-color 160ms ease, box-shadow 160ms ease; }
    .dot.up { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .dot.degraded { background: var(--amber); box-shadow: 0 0 10px var(--amber); }
    .dot.down { background: var(--red); box-shadow: 0 0 10px var(--red); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 30px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 44px; height: 44px; min-height: 44px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    ${Ms}
    .auth-panel { width: min(440px, 100%); margin: auto; }
    .access-panels { margin-top: 18px; }
    .access-resource { align-items: end; }
    .access-form { display: grid; flex: 1; grid-template-columns: 1fr 1fr auto; align-items: end; gap: 9px; }
    .compact-form { border-top: 1px solid var(--divider); }
    .compact-form h3 { margin: 0; }
    .token-value { margin: 14px; overflow-wrap: anywhere; }
    .token-value code { display: block; margin: 8px 0; }
    .overview-top { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-bottom: 18px; }
    .page-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .summary { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .metric.down.active span, .metric.down.active strong { color: var(--red); }
    .panel { border-radius: 16px; overflow: hidden; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .badge.up { border-color: var(--green); color: var(--green); }
    .badge.down { border-color: var(--red); color: var(--red); }
    .transition-main { display: flex; align-items: center; gap: 12px; }
    .channel-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .channel-summary { min-width: 0; }
    .channel-title, .channel-actions { display: flex; align-items: center; gap: 10px; }
    .channel-summary code { display: block; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .channel-actions .switch span { font-size: 12px; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .alert-history { margin-bottom: 20px; }
    .alert-filters { display: grid; grid-template-columns: minmax(180px, 1fr) repeat(3, minmax(120px, auto)); gap: 10px; padding: 14px 20px; border-bottom: 1px solid var(--line); }
    .alert-filters label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    .alert-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .alert-summary { display: grid; min-width: 0; gap: 4px; }
    .alert-summary code, .alert-summary .meta { font-size: 11px; }
    .alert-summary .meta { color: var(--muted); white-space: normal; }
    .alert-actions { display: flex; align-items: center; gap: 8px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 24px; height: 24px; accent-color: var(--green); }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .node-target { cursor: pointer; }
    .state { width: 10px; height: 10px; border-radius: 50%; color: var(--amber); background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .target-title { display: flex; align-items: center; gap: 8px; margin-bottom: 3px; }
    .target-title h3 { margin: 0; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open] { opacity: 1; transform: translateY(0) scale(1); }
    dialog::backdrop { background: var(--backdrop); backdrop-filter: blur(5px); opacity: 0; transition: opacity 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open]::backdrop { opacity: 1; }
    @starting-style {
      dialog[open] { opacity: 0; transform: translateY(8px) scale(.985); }
      dialog[open]::backdrop { opacity: 0; }
    }
    .dialog-head { position: relative; padding: 20px 58px 15px 22px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    [hidden] { display: none !important; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, a:focus-visible, .target:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; }
    .check { display: flex; align-items: center; gap: 8px; } .check input { width: 18px; min-height: 18px; height: 18px; flex: none; }
    .channel-fields { display: grid; gap: 10px; margin: 8px 0 0; border: 0; padding: 0; }
    .channel-fields legend { display: flex; width: 100%; align-items: center; gap: 12px; margin: 0 0 4px; padding: 0; color: var(--text); font-size: 14px; font-weight: 400; text-align: center; }
    .channel-fields legend::before, .channel-fields legend::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    form .badge { font-size: 12px; }
    .channel-options { display: grid; gap: 6px; }
    .channel-options .check { min-height: 36px; border-radius: 8px; padding: 5px 8px; background: var(--panel-2); }
    .channel-options .badge { margin-left: auto; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .switch input { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch input::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch input:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch input:checked::after { background: var(--button-text); transform: translateX(18px); }
    footer { display: flex; flex: 0 0 auto; width: calc(100% - 48px); max-width: 1152px; flex-direction: column; align-items: center; justify-content: center; gap: 8px; margin: 0 auto; border-top: 1px solid var(--line); padding: 20px 0 24px; color: var(--muted); font-size: 12px; }
    .footer-links, .footer-powered { display: flex; align-items: center; justify-content: center; flex-wrap: wrap; gap: 10px; text-align: center; }
    footer a { display: inline-flex; align-items: center; gap: 5px; border-radius: 4px; color: var(--green); text-decoration: underline; text-underline-offset: 3px; transition: color 160ms ease; }
    footer a:hover { color: var(--text); }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .chart-plot { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 7px; }
    .chart-scale { display: flex; height: 140px; flex-direction: column; justify-content: space-between; padding: 1px 0 7px; color: var(--muted); font-size: 9px; text-align: right; }
    .history-chart { display: flex; height: 140px; align-items: flex-end; gap: 3px; padding: 14px 10px 8px; border: 1px solid var(--line); border-radius: 10px; background: var(--input-bg); }
    .history-bar { flex: 1; min-width: 3px; max-width: 16px; border-radius: 3px 3px 1px 1px; opacity: .82; transform-origin: bottom; transition: opacity 120ms ease, transform 120ms ease; }
    .history-bar:hover { opacity: 1; transform: scaleX(1.15); }
    .history-bar.up { background: var(--green); }
    .history-bar.down { background: var(--red); }
    .chart-axis { justify-content: space-between; margin: 5px 0 0 45px; color: var(--muted); font-size: 10px; }
    .chart-legend { justify-content: flex-end; gap: 12px; margin-top: 9px; color: var(--muted); font-size: 10px; }
    .chart-legend span { gap: 5px; }
    .chart-legend i { width: 7px; height: 7px; border-radius: 2px; }
    .chart-legend .up { background: var(--green); }
    .chart-legend .down { background: var(--red); }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    :host([data-theme="bright"]) {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --panel-2: #eef5f1;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #c53434;
        --amber: #9a6700;
        --page-background:
          radial-gradient(circle at 12% -5%, #d9f2e4 0, transparent 32%),
          linear-gradient(145deg, #fbfdfc 0%, #f3f8f5 55%, #edf5f1 100%);
        --brand-shadow: #159e5240;
        --nav-bg: #ffffffcc;
        --active-bg: #e4efe9;
        --button-border: #16764b;
        --button-bg: #087a49;
        --button-text: #ffffff;
        --button-hover-border: #075f3a;
        --panel-surface: #ffffffeb;
        --panel-shadow: #2745381a;
        --divider: #e3ebe7;
        --badge-border: #a6beb2;
        --badge-text: #426356;
        --row-hover: #e9f4ee;
        --notice-border: #e2aaa6;
        --notice-bg: #fff0ef;
        --notice-text: #9f2922;
        --bulk-bg: #e8f4ed;
        --dialog-shadow: #233b3050;
        --backdrop: #17251f66;
        --input-bg: #ffffff;
        --focus: #168655;
        --danger-text: #b42318;
        --danger-border: #dda29d;
        --warning-bg: #fff1bd;
        --warning-text: #805b00;
        --warning-border: #d4aa36;
        --join-bg: #eef8f2;
    }
    @media (prefers-reduced-motion: reduce) {
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select, .help-tooltip-trigger, .help-tooltip { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      header { grid-template-columns: minmax(0, 1fr) auto; row-gap: 16px; }
      header > nav { display: flex; grid-column: 1 / -1; grid-row: 2; justify-self: center; }
      .overview-top { grid-template-columns: 1fr; }
      .page-columns { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target-wrap { align-items: start; padding-left: 14px; } .select-target { align-self: start; margin-top: 0; } .target { grid-template-columns: auto minmax(0, 1fr); gap: 10px; padding: 12px 14px 12px 10px; }
      .target-side { grid-column: 2; display: grid; grid-template-columns: minmax(88px, 1fr) auto; width: 100%; gap: 18px; margin-top: 4px; } .target > .state { align-self: start; margin-top: 5px; } .mini-chart { width: 100%; max-width: 140px; height: 28px; }
      .latency { min-width: 72px; text-align: right; }
      .channel-resource { grid-template-columns: 1fr; }
      .alert-filters { grid-template-columns: 1fr 1fr; }
      .alert-resource { grid-template-columns: 1fr; }
      .alert-actions { margin-top: 8px; }
      .channel-actions { justify-content: space-between; margin-top: 10px; }
      .access-form { grid-template-columns: 1fr; }
    }
  `;De=Bs([kt("upgrid-app")],De);
